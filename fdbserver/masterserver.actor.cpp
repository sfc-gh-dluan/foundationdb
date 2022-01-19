/*
 * masterserver.actor.cpp
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

#include <iterator>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbserver/ConflictSet.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/DBCoreState.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/ProxyCommitData.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Trace.h"
#include "fdbclient/VersionVector.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MasterData : NonCopyable, ReferenceCounted<MasterData> {
	UID dbgid;

	Version lastEpochEnd, // The last version in the old epoch not (to be) rolled back in this recovery
	    recoveryTransactionVersion; // The first version in this epoch

	NotifiedVersion prevTLogVersion; // Order of transactions to tlogs

	NotifiedVersion liveCommittedVersion; // The largest live committed version reported by commit proxies.
	bool databaseLocked;
	Optional<Value> proxyMetadataVersion;
	Version minKnownCommittedVersion;

	ServerCoordinators coordinators;

	Version version; // The last version assigned to a proxy by getVersion()
	double lastVersionTime;
	IKeyValueStore* txnStateStore;

	std::vector<CommitProxyInterface> commitProxies;
	std::map<UID, CommitProxyVersionReplies> lastCommitProxyVersionReplies;

	MasterInterface myInterface;

	AsyncVar<Standalone<VectorRef<ResolverMoveRef>>> resolverChanges;
	Version resolverChangesVersion;
	std::set<UID> resolverNeedingChanges;

	bool forceRecovery;

	// Captures the latest commit version targeted for each storage server in the cluster.
	// @todo We need to ensure that the latest commit versions of storage servers stay
	// up-to-date in the presence of key range splits/merges.
	VersionVector ssVersionVector;

	CounterCollection cc;
	Counter getCommitVersionRequests;
	Counter getLiveCommittedVersionRequests;
	Counter reportLiveCommittedVersionRequests;
	// This counter gives an estimate of the number of non-empty peeks that storage servers
	// should do from tlogs (in the worst case, ignoring blocking peek timeouts).
	Counter versionVectorTagUpdates;
	Counter waitForPrevCommitRequests;
	Counter nonWaitForPrevCommitRequests;
	LatencySample versionVectorSizeOnCVReply;
	LatencySample waitForPrevLatencies;

	Future<Void> logger;
        PromiseStream<Future<Void>> addActor;

	MasterData(Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
	           MasterInterface const& myInterface,
	           ServerCoordinators const& coordinators,
	           ClusterControllerFullInterface const& clusterController,
	           Standalone<StringRef> const& dbId,
	           PromiseStream<Future<Void>> const& addActor,
	           bool forceRecovery)
          : dbgid(myInterface.id()), lastEpochEnd(invalidVersion), recoveryTransactionVersion(invalidVersion),
            liveCommittedVersion(invalidVersion), databaseLocked(false), minKnownCommittedVersion(invalidVersion),
            coordinators(coordinators), version(invalidVersion), lastVersionTime(0), txnStateStore(nullptr),
            myInterface(myInterface), forceRecovery(forceRecovery), cc("Master", dbgid.toString()),
	    getCommitVersionRequests("GetCommitVersionRequests", cc),
	    getLiveCommittedVersionRequests("GetLiveCommittedVersionRequests", cc),
	    reportLiveCommittedVersionRequests("ReportLiveCommittedVersionRequests", cc),
	    versionVectorTagUpdates("VersionVectorTagUpdates", cc),
	    waitForPrevCommitRequests("WaitForPrevCommitRequests", cc),
	    nonWaitForPrevCommitRequests("NonWaitForPrevCommitRequests", cc),
            addActor(addActor),
	    versionVectorSizeOnCVReply("VersionVectorSizeOnCVReply",
	                               dbgid,
	                               SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                               SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    waitForPrevLatencies("WaitForPrevLatencies",
	                         dbgid,
	                         SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                         SERVER_KNOBS->LATENCY_SAMPLE_SIZE) {
		logger = traceCounters("MasterMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "MasterMetrics");
		if (forceRecovery && !myInterface.locality.dcId().present()) {
			TraceEvent(SevError, "ForcedRecoveryRequiresDcID").log();
			forceRecovery = false;
                }
	}
	~MasterData() {
		if (txnStateStore)
			txnStateStore->close();
	}
};

ACTOR Future<Void> getVersion(Reference<MasterData> self, GetCommitVersionRequest req) {
	state Span span("M:getVersion"_loc, { req.spanContext });
	state std::map<UID, CommitProxyVersionReplies>::iterator proxyItr =
	    self->lastCommitProxyVersionReplies.find(req.requestingProxy); // lastCommitProxyVersionReplies never changes

	++self->getCommitVersionRequests;

	if (proxyItr == self->lastCommitProxyVersionReplies.end()) {
		// Request from invalid proxy (e.g. from duplicate recruitment request)
		req.reply.send(Never());
		return Void();
	}

	TEST(proxyItr->second.latestRequestNum.get() < req.requestNum - 1); // Commit version request queued up
	wait(proxyItr->second.latestRequestNum.whenAtLeast(req.requestNum - 1));

	auto itr = proxyItr->second.replies.find(req.requestNum);
	if (itr != proxyItr->second.replies.end()) {
		TEST(true); // Duplicate request for sequence
		req.reply.send(itr->second);
	} else if (req.requestNum <= proxyItr->second.latestRequestNum.get()) {
		TEST(true); // Old request for previously acknowledged sequence - may be impossible with current FlowTransport
		ASSERT(req.requestNum <
		       proxyItr->second.latestRequestNum.get()); // The latest request can never be acknowledged
		req.reply.send(Never());
	} else {
		GetCommitVersionReply rep;

		if (self->version == invalidVersion) {
			self->lastVersionTime = now();
			self->version = self->recoveryTransactionVersion;
			rep.prevVersion = self->lastEpochEnd;

		} else {
			double t1 = now();
			if (BUGGIFY) {
				t1 = self->lastVersionTime;
			}
			rep.prevVersion = self->version;
			self->version +=
			    std::max<Version>(1,
			                      std::min<Version>(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS,
			                                        SERVER_KNOBS->VERSIONS_PER_SECOND * (t1 - self->lastVersionTime)));

			TEST(self->version - rep.prevVersion == 1); // Minimum possible version gap

			bool maxVersionGap = self->version - rep.prevVersion == SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
			TEST(maxVersionGap); // Maximum possible version gap
			self->lastVersionTime = t1;

			if (self->resolverNeedingChanges.count(req.requestingProxy)) {
				rep.resolverChanges = self->resolverChanges.get();
				rep.resolverChangesVersion = self->resolverChangesVersion;
				self->resolverNeedingChanges.erase(req.requestingProxy);

				if (self->resolverNeedingChanges.empty())
					self->resolverChanges.set(Standalone<VectorRef<ResolverMoveRef>>());
			}
		}

		rep.version = self->version;
		rep.requestNum = req.requestNum;

		proxyItr->second.replies.erase(proxyItr->second.replies.begin(),
		                               proxyItr->second.replies.upper_bound(req.mostRecentProcessedRequestNum));
		proxyItr->second.replies[req.requestNum] = rep;
		ASSERT(rep.prevVersion >= 0);

		req.reply.send(rep);

		ASSERT(proxyItr->second.latestRequestNum.get() == req.requestNum - 1);
		proxyItr->second.latestRequestNum.set(req.requestNum);
	}

	return Void();
}

ACTOR Future<Void> provideVersions(Reference<MasterData> self) {
	state ActorCollection versionActors(false);

	for (auto& p : self->commitProxies)
		self->lastCommitProxyVersionReplies[p.id()] = CommitProxyVersionReplies();

	loop {
		choose {
			when(GetCommitVersionRequest req = waitNext(self->myInterface.getCommitVersion.getFuture())) {
				versionActors.add(getVersion(self, req));
			}
			when(wait(versionActors.getResult())) {}
		}
	}
}

void updateLiveCommittedVersion(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, req.minKnownCommittedVersion);

	if (req.version > self->liveCommittedVersion.get()) {
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR && req.writtenTags.present()) {
			// TraceEvent("Received ReportRawCommittedVersionRequest").detail("Version",req.version);
			self->ssVersionVector.setVersion(req.writtenTags.get(), req.version);
			self->versionVectorTagUpdates += req.writtenTags.get().size();
		}
		self->databaseLocked = req.locked;
		self->proxyMetadataVersion = req.metadataVersion;
		// Note the set call switches context to any waiters on liveCommittedVersion before continuing.
		self->liveCommittedVersion.set(req.version);
	}
	++self->reportLiveCommittedVersionRequests;
}

ACTOR Future<Void> waitForPrev(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	state double startTime = now();
	wait(self->liveCommittedVersion.whenAtLeast(req.prevVersion.get()));
	double latency = now() - startTime;
	self->waitForPrevLatencies.addMeasurement(latency);
	++self->waitForPrevCommitRequests;
	updateLiveCommittedVersion(self, req);
	req.reply.send(Void());
	return Void();
}

ACTOR Future<Void> serveLiveCommittedVersion(Reference<MasterData> self) {
	loop {
		choose {
			when(GetRawCommittedVersionRequest req = waitNext(self->myInterface.getLiveCommittedVersion.getFuture())) {
				if (req.debugID.present())
					g_traceBatch.addEvent("TransactionDebug",
					                      req.debugID.get().first(),
					                      "MasterServer.serveLiveCommittedVersion.GetRawCommittedVersion");

				if (self->liveCommittedVersion.get() == invalidVersion) {
					self->liveCommittedVersion.set(self->recoveryTransactionVersion);
				}
				++self->getLiveCommittedVersionRequests;
				GetRawCommittedVersionReply reply;
				reply.version = self->liveCommittedVersion.get();
				reply.locked = self->databaseLocked;
				reply.metadataVersion = self->proxyMetadataVersion;
				reply.minKnownCommittedVersion = self->minKnownCommittedVersion;
				self->ssVersionVector.getDelta(req.maxVersion, reply.ssVersionVectorDelta);
				self->versionVectorSizeOnCVReply.addMeasurement(reply.ssVersionVectorDelta.size());
				req.reply.send(reply);
			}
			when(ReportRawCommittedVersionRequest req =
			         waitNext(self->myInterface.reportLiveCommittedVersion.getFuture())) {
				if (SERVER_KNOBS->ENABLE_VERSION_VECTOR && req.prevVersion.present() &&
				    (self->liveCommittedVersion.get() != invalidVersion) &&
				    (self->liveCommittedVersion.get() < req.prevVersion.get())) {
					self->addActor.send(waitForPrev(self, req));
				} else {
					updateLiveCommittedVersion(self, req);
					++self->nonWaitForPrevCommitRequests;
					req.reply.send(Void());
				}
			}
		}
	}
}

ACTOR Future<Void> updateRecoveryData(Reference<MasterData> self) {
	loop {
		choose {
			when(UpdateRecoveryDataRequest req = waitNext(self->myInterface.updateRecoveryData.getFuture())) {
				TraceEvent("UpdateRecoveryData", self->dbgid)
				    .detail("RecoveryTxnVersion", req.recoveryTransactionVersion)
				    .detail("LastEpochEnd", req.lastEpochEnd)
				    .detail("NumCommitProxies", req.commitProxies.size());

				if (self->recoveryTransactionVersion == invalidVersion ||
				    req.recoveryTransactionVersion > self->recoveryTransactionVersion) {
					self->recoveryTransactionVersion = req.recoveryTransactionVersion;
				}
				if (self->lastEpochEnd == invalidVersion || req.lastEpochEnd > self->lastEpochEnd) {
					self->lastEpochEnd = req.lastEpochEnd;
				}
				if (req.commitProxies.size() > 0) {
					self->commitProxies = req.commitProxies;
					self->lastCommitProxyVersionReplies.clear();

					for (auto& p : self->commitProxies) {
						self->lastCommitProxyVersionReplies[p.id()] = CommitProxyVersionReplies();
					}
				}
				req.reply.send(Void());
			}
		}
	}
}

static std::set<int> const& normalMasterErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_tlog_stopped);
		s.insert(error_code_tlog_failed);
		s.insert(error_code_commit_proxy_failed);
		s.insert(error_code_grv_proxy_failed);
		s.insert(error_code_resolver_failed);
		s.insert(error_code_backup_worker_failed);
		s.insert(error_code_recruitment_failed);
		s.insert(error_code_no_more_servers);
		s.insert(error_code_cluster_recovery_failed);
		s.insert(error_code_coordinated_state_conflict);
		s.insert(error_code_master_max_versions_in_flight);
		s.insert(error_code_worker_removed);
		s.insert(error_code_new_coordinators_timed_out);
		s.insert(error_code_broken_promise);
	}
	return s;
}

ACTOR Future<Void> masterServer(MasterInterface mi,
                                Reference<AsyncVar<ServerDBInfo> const> db,
                                Reference<AsyncVar<Optional<ClusterControllerFullInterface>> const> ccInterface,
                                ServerCoordinators coordinators,
                                LifetimeToken lifetime,
                                bool forceRecovery) {
	state Future<Void> ccTimeout = delay(SERVER_KNOBS->CC_INTERFACE_TIMEOUT);
	while (!ccInterface->get().present() || db->get().clusterInterface != ccInterface->get().get()) {
		wait(ccInterface->onChange() || db->onChange() || ccTimeout);
		if (ccTimeout.isReady()) {
			TraceEvent("MasterTerminated", mi.id())
			    .detail("Reason", "Timeout")
			    .detail("CCInterface", ccInterface->get().present() ? ccInterface->get().get().id() : UID())
			    .detail("DBInfoInterface", db->get().clusterInterface.id());
			return Void();
		}
	}

	state Future<Void> onDBChange = Void();
	state PromiseStream<Future<Void>> addActor;
	state Reference<MasterData> self(new MasterData(
	    db, mi, coordinators, db->get().clusterInterface, LiteralStringRef(""), addActor, forceRecovery));
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(traceRole(Role::MASTER, mi.id()));
	addActor.send(provideVersions(self));
	addActor.send(serveLiveCommittedVersion(self));
	addActor.send(updateRecoveryData(self));

	TEST(!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id())); // Master born doomed
	TraceEvent("MasterLifetime", self->dbgid).detail("LifetimeToken", lifetime.toString());

	try {
		loop choose {
			when(wait(onDBChange)) {
				onDBChange = db->onChange();
				if (!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id())) {
					TraceEvent("MasterTerminated", mi.id())
					    .detail("Reason", "LifetimeToken")
					    .detail("MyToken", lifetime.toString())
					    .detail("CurrentToken", db->get().masterLifetime.toString());
					TEST(true); // Master replaced, dying
					if (BUGGIFY)
						wait(delay(5));
					throw worker_removed();
				}
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		state Error err = e;
		if (e.code() != error_code_actor_cancelled) {
			wait(delay(0.0));
		}
		while (!addActor.isEmpty()) {
			addActor.getFuture().pop();
		}

		TEST(err.code() == error_code_tlog_failed); // Master: terminated due to tLog failure
		TEST(err.code() == error_code_commit_proxy_failed); // Master: terminated due to commit proxy failure
		TEST(err.code() == error_code_grv_proxy_failed); // Master: terminated due to GRV proxy failure
		TEST(err.code() == error_code_resolver_failed); // Master: terminated due to resolver failure
		TEST(err.code() == error_code_backup_worker_failed); // Master: terminated due to backup worker failure

		if (normalMasterErrors().count(err.code())) {
			TraceEvent("MasterTerminated", mi.id()).error(err);
			return Void();
		}
		throw err;
	}
}
