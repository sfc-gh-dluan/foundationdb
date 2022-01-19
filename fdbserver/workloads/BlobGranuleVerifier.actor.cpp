/*
 * BlobGranuleVerifier.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <map>
#include <utility>
#include <vector>

#include "contrib/fmt-8.0.1/include/fmt/format.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BGV_DEBUG false

/*
 * This workload is designed to verify the correctness of the blob data produced by the blob workers.
 * As a read-only validation workload, it can piggyback off of other write or read/write workloads.
 * To verify the data outside FDB's 5 second MVCC window, it tests time travel reads by doing an initial comparison at
 * the latest read version, and then waiting a period of time to re-read the data from blob.
 * To catch availability issues with the blob worker, it does a request to each granule at the end of the test.
 */
struct BlobGranuleVerifierWorkload : TestWorkload {
	bool doSetup;
	double minDelay;
	double maxDelay;
	double testDuration;
	double timeTravelLimit;
	uint64_t timeTravelBufferSize;
	int threads;
	int64_t errors = 0;
	int64_t mismatches = 0;
	int64_t initialReads = 0;
	int64_t timeTravelReads = 0;
	int64_t timeTravelTooOld = 0;
	int64_t rowsRead = 0;
	int64_t bytesRead = 0;
	std::vector<Future<Void>> clients;

	Reference<BackupContainerFileSystem> bstore;
	AsyncVar<Standalone<VectorRef<KeyRangeRef>>> granuleRanges;

	BlobGranuleVerifierWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doSetup = !clientId; // only do this on the "first" client
		// FIXME: don't do the delay in setup, as that delays the start of all workloads
		minDelay = getOption(options, LiteralStringRef("minDelay"), 0.0);
		maxDelay = getOption(options, LiteralStringRef("maxDelay"), 0.0);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
		timeTravelLimit = getOption(options, LiteralStringRef("timeTravelLimit"), testDuration);
		timeTravelBufferSize = getOption(options, LiteralStringRef("timeTravelBufferSize"), 100000000);
		threads = getOption(options, LiteralStringRef("threads"), 1);
		ASSERT(threads >= 1);

		if (BGV_DEBUG) {
			printf("Initializing Blob Granule Verifier s3 stuff\n");
		}
		try {
			if (g_network->isSimulated()) {

				if (BGV_DEBUG) {
					printf("Blob Granule Verifier constructing simulated backup container\n");
				}
				bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/");
			} else {
				if (BGV_DEBUG) {
					printf("Blob Granule Verifier constructing backup container from %s\n",
					       SERVER_KNOBS->BG_URL.c_str());
				}
				bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
				if (BGV_DEBUG) {
					printf("Blob Granule Verifier constructed backup container\n");
				}
			}
		} catch (Error& e) {
			if (BGV_DEBUG) {
				printf("Blob Granule Verifier got backup container init error %s\n", e.name());
			}
			throw e;
		}
	}

	// FIXME: run the actual FDBCLI command instead of copy/pasting its implementation
	// Sets the whole user keyspace to be blobified
	ACTOR Future<Void> setUpBlobRange(Database cx, Future<Void> waitForStart) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		wait(waitForStart);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(krmSetRange(tr, blobRangeKeys.begin, KeyRange(normalKeys), LiteralStringRef("1")));
				wait(tr->commit());
				if (BGV_DEBUG) {
					printf("Successfully set up blob granule range for normalKeys\n");
				}
				TraceEvent("BlobGranuleVerifierSetup");
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	std::string description() const override { return "BlobGranuleVerifier"; }
	Future<Void> setup(Database const& cx) override {
		if (!CLIENT_KNOBS->ENABLE_BLOB_GRANULES) {
			return Void();
		}

		if (doSetup) {
			double initialDelay = deterministicRandom()->random01() * (maxDelay - minDelay) + minDelay;
			if (BGV_DEBUG) {
				printf("BGW setup initial delay of %.3f\n", initialDelay);
			}
			return setUpBlobRange(cx, delay(initialDelay));
		}
		return delay(0);
	}

	ACTOR Future<Void> findGranules(Database cx, BlobGranuleVerifierWorkload* self) {
		loop {
			state Transaction tr(cx);
			loop {
				try {
					Standalone<VectorRef<KeyRangeRef>> allGranules = wait(tr.getBlobGranuleRanges(normalKeys));
					self->granuleRanges.set(allGranules);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			wait(delay(deterministicRandom()->random01() * 10.0));
		}
	}

	// assumes we can read the whole range in one transaction at a single version
	ACTOR Future<std::pair<RangeResult, Version>> readFromFDB(Database cx, KeyRange range) {
		state Version v;
		state RangeResult out;
		state Transaction tr(cx);
		state KeyRange currentRange = range;
		loop {
			try {
				RangeResult r = wait(tr.getRange(currentRange, CLIENT_KNOBS->TOO_MANY));
				out.arena().dependsOn(r.arena());
				out.append(out.arena(), r.begin(), r.size());
				if (r.more) {
					currentRange = KeyRangeRef(keyAfter(r.back().key), currentRange.end);
				} else {
					Version _v = wait(tr.getReadVersion());
					v = _v;
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return std::pair(out, v);
	}

	// FIXME: typedef this pair type and/or chunk list
	ACTOR Future<std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>>
	readFromBlob(Database cx, BlobGranuleVerifierWorkload* self, KeyRange range, Version version) {
		state RangeResult out;
		state Standalone<VectorRef<BlobGranuleChunkRef>> chunks;
		state Transaction tr(cx);

		loop {
			try {
				Standalone<VectorRef<BlobGranuleChunkRef>> chunks_ = wait(tr.readBlobGranules(range, 0, version));
				chunks = chunks_;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		for (const BlobGranuleChunkRef& chunk : chunks) {
			RangeResult chunkRows = wait(readBlobGranule(chunk, range, version, self->bstore));
			out.arena().dependsOn(chunkRows.arena());
			out.append(out.arena(), chunkRows.begin(), chunkRows.size());
		}
		return std::pair(out, chunks);
	}

	bool compareResult(RangeResult fdb,
	                   std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob,
	                   KeyRange range,
	                   Version v,
	                   bool initialRequest) {
		bool correct = fdb == blob.first;
		if (!correct) {
			mismatches++;
			TraceEvent ev(SevError, "GranuleMismatch");
			ev.detail("RangeStart", range.begin)
			    .detail("RangeEnd", range.end)
			    .detail("Version", v)
			    .detail("RequestType", initialRequest ? "RealTime" : "TimeTravel")
			    .detail("FDBSize", fdb.size())
			    .detail("BlobSize", blob.first.size());

			if (BGV_DEBUG) {
				fmt::print("\nMismatch for [{0} - {1}) @ {2} ({3}). F({4}) B({5}):\n",
				           range.begin.printable(),
				           range.end.printable(),
				           v,
				           initialRequest ? "RealTime" : "TimeTravel",
				           fdb.size(),
				           blob.first.size());

				Optional<KeyValueRef> lastCorrect;
				for (int i = 0; i < std::max(fdb.size(), blob.first.size()); i++) {
					if (i >= fdb.size() || i >= blob.first.size() || fdb[i] != blob.first[i]) {
						printf("  Found mismatch at %d.\n", i);
						if (lastCorrect.present()) {
							printf("    last correct: %s=%s\n",
							       lastCorrect.get().key.printable().c_str(),
							       lastCorrect.get().value.printable().c_str());
						}
						if (i < fdb.size()) {
							printf(
							    "    FDB: %s=%s\n", fdb[i].key.printable().c_str(), fdb[i].value.printable().c_str());
						} else {
							printf("    FDB: <missing>\n");
						}
						if (i < blob.first.size()) {
							printf("    BLB: %s=%s\n",
							       blob.first[i].key.printable().c_str(),
							       blob.first[i].value.printable().c_str());
						} else {
							printf("    BLB: <missing>\n");
						}
						printf("\n");
						break;
					}
					if (i < fdb.size()) {
						lastCorrect = fdb[i];
					} else {
						lastCorrect = blob.first[i];
					}
				}

				printf("Chunks:\n");
				for (auto& chunk : blob.second) {
					printf("[%s - %s)\n",
					       chunk.keyRange.begin.printable().c_str(),
					       chunk.keyRange.end.printable().c_str());

					printf("  SnapshotFile:\n    %s\n",
					       chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str() : "<none>");
					printf("  DeltaFiles:\n");
					for (auto& df : chunk.deltaFiles) {
						printf("    %s\n", df.toString().c_str());
					}
					printf("  Deltas: (%d)", chunk.newDeltas.size());
					if (chunk.newDeltas.size() > 0) {
						fmt::print(" with version [{0} - {1}]",
						           chunk.newDeltas[0].version,
						           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
					}
					fmt::print("  IncludedVersion: {}\n", chunk.includedVersion);
				}
				printf("\n");
			}
		}
		return correct;
	}

	struct OldRead {
		KeyRange range;
		Version v;
		RangeResult oldResult;

		OldRead() {}
		OldRead(KeyRange range, Version v, RangeResult oldResult) : range(range), v(v), oldResult(oldResult) {}
	};

	ACTOR Future<Void> verifyGranules(Database cx, BlobGranuleVerifierWorkload* self) {
		state double last = now();
		state double endTime = last + self->testDuration;
		state std::map<double, OldRead> timeTravelChecks;
		state int64_t timeTravelChecksMemory = 0;

		TraceEvent("BlobGranuleVerifierStart");
		if (BGV_DEBUG) {
			printf("BGV thread starting\n");
		}

		// wait for first set of ranges to be loaded
		wait(self->granuleRanges.onChange());

		if (BGV_DEBUG) {
			printf("BGV got ranges\n");
		}

		loop {
			try {
				state double currentTime = now();
				state std::map<double, OldRead>::iterator timeTravelIt = timeTravelChecks.begin();
				while (timeTravelIt != timeTravelChecks.end() && currentTime >= timeTravelIt->first) {
					state OldRead oldRead = timeTravelIt->second;
					timeTravelChecksMemory -= oldRead.oldResult.expectedSize();
					timeTravelIt = timeTravelChecks.erase(timeTravelIt);
					// advance iterator before doing read, so if it gets error we don't retry it

					try {
						std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> reReadResult =
						    wait(self->readFromBlob(cx, self, oldRead.range, oldRead.v));
						self->compareResult(oldRead.oldResult, reReadResult, oldRead.range, oldRead.v, false);
						self->timeTravelReads++;
					} catch (Error& e) {
						if (e.code() == error_code_transaction_too_old) {
							self->timeTravelTooOld++;
							// TODO: add debugging info for when this is a failure
						}
					}
				}

				// pick a random range
				int rIndex = deterministicRandom()->randomInt(0, self->granuleRanges.get().size());
				state KeyRange range = self->granuleRanges.get()[rIndex];

				state std::pair<RangeResult, Version> fdb = wait(self->readFromFDB(cx, range));
				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
				    wait(self->readFromBlob(cx, self, range, fdb.second));
				if (self->compareResult(fdb.first, blob, range, fdb.second, true)) {
					// TODO: bias for immediately re-reading to catch rollback cases
					double reReadTime = currentTime + deterministicRandom()->random01() * self->timeTravelLimit;
					int memory = fdb.first.expectedSize();
					if (reReadTime <= endTime &&
					    timeTravelChecksMemory + memory <= (self->timeTravelBufferSize / self->threads)) {
						timeTravelChecks[reReadTime] = OldRead(range, fdb.second, fdb.first);
						timeTravelChecksMemory += memory;
					}
				}
				self->rowsRead += fdb.first.size();
				self->bytesRead += fdb.first.expectedSize();
				self->initialReads++;

			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw;
				}
				if (e.code() != error_code_transaction_too_old && e.code() != error_code_wrong_shard_server &&
				    BGV_DEBUG) {
					printf("BGVerifier got unexpected error %s\n", e.name());
				}
				self->errors++;
			}
			// wait(poisson(&last, 5.0));
			wait(poisson(&last, 0.1));
		}
	}

	Future<Void> start(Database const& cx) override {
		if (!CLIENT_KNOBS->ENABLE_BLOB_GRANULES) {
			return Void();
		}

		clients.reserve(threads + 1);
		clients.push_back(timeout(findGranules(cx, this), testDuration, Void()));
		for (int i = 0; i < threads; i++) {
			clients.push_back(
			    timeout(reportErrors(verifyGranules(cx, this), "BlobGranuleVerifier"), testDuration, Void()));
		}
		return delay(testDuration);
	}

	ACTOR Future<bool> _check(Database cx, BlobGranuleVerifierWorkload* self) {
		// check error counts, and do an availability check at the end

		state Transaction tr(cx);
		state Version readVersion = wait(tr.getReadVersion());
		state int checks = 0;

		state KeyRange last;
		state bool availabilityPassed = true;
		state Standalone<VectorRef<KeyRangeRef>> allRanges = self->granuleRanges.get();
		for (auto& range : allRanges) {
			state KeyRange r = range;
			state PromiseStream<Standalone<BlobGranuleChunkRef>> chunkStream;
			if (BGV_DEBUG) {
				fmt::print("Final availability check [{0} - {1}) @ {2}\n",
				           r.begin.printable(),
				           r.end.printable(),
				           readVersion);
			}

			try {
				loop {
					tr.reset();
					try {
						Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
						    wait(tr.readBlobGranules(r, 0, readVersion));
						ASSERT(chunks.size() > 0);
						last = chunks.back().keyRange;
						checks += chunks.size();
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					break;
				}
				if (BGV_DEBUG) {
					fmt::print("BG Verifier failed final availability check for [{0} - {1}) @ {2} with error {3}. Last "
					           "Success=[{4} - {5})\n",
					           r.begin.printable(),
					           r.end.printable(),
					           readVersion,
					           e.name(),
					           last.begin.printable(),
					           last.end.printable());
				}
				availabilityPassed = false;
				break;
			}
		}
		fmt::print("Blob Granule Verifier finished with:\n");
		fmt::print("  {} successful final granule checks\n", checks);
		fmt::print("  {} failed final granule checks\n", availabilityPassed ? 0 : 1);
		fmt::print("  {} mismatches\n", self->mismatches);
		fmt::print("  {} time travel too old\n", self->timeTravelTooOld);
		fmt::print("  {} errors\n", self->errors);
		fmt::print("  {} initial reads\n", self->initialReads);
		fmt::print("  {} time travel reads\n", self->timeTravelReads);
		fmt::print("  {} rows\n", self->rowsRead);
		fmt::print("  {} bytes\n", self->bytesRead);
		// FIXME: add above as details
		TraceEvent("BlobGranuleVerifierChecked");
		return availabilityPassed && self->mismatches == 0 && checks > 0 && self->timeTravelTooOld == 0;
	}

	Future<bool> check(Database const& cx) override {
		if (!CLIENT_KNOBS->ENABLE_BLOB_GRANULES) {
			return true;
		}

		return _check(cx, this);
	}
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleVerifierWorkload> BlobGranuleVerifierWorkloadFactory("BlobGranuleVerifier");
