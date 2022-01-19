/*
 * ChangeFeedCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "contrib/fmt-8.0.1/include/fmt/format.h"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> changeFeedList(Database db) {
	state ReadYourWritesTransaction tr(db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			RangeResult result = wait(tr.getRange(changeFeedKeys, CLIENT_KNOBS->TOO_MANY));
			// shouldn't have many quarantined TSSes
			ASSERT(!result.more);
			printf("Found %d range feeds%s\n", result.size(), result.size() == 0 ? "." : ":");
			for (auto& it : result) {
				auto range = std::get<0>(decodeChangeFeedValue(it.value));
				printf("  %s: %s - %s\n",
				       it.key.removePrefix(changeFeedPrefix).toString().c_str(),
				       range.begin.toString().c_str(),
				       range.end.toString().c_str());
			}
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

} // namespace

namespace fdb_cli {

ACTOR Future<Void> requestVersionUpdate(Database localDb, Reference<ChangeFeedData> feedData) {
	loop {
		wait(delay(5.0));
		Transaction tr(localDb);
		state Version ver = wait(tr.getReadVersion());
		fmt::print("Requesting version {}\n", ver);
		wait(feedData->whenAtLeast(ver));
		fmt::print("Feed at version {}\n", ver);
	}
}

ACTOR Future<bool> changeFeedCommandActor(Database localDb, std::vector<StringRef> tokens, Future<Void> warn) {
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return false;
	}
	if (tokencmp(tokens[1], "list")) {
		if (tokens.size() != 2) {
			printUsage(tokens[0]);
			return false;
		}
		wait(changeFeedList(localDb));
		return true;
	} else if (tokencmp(tokens[1], "register")) {
		if (tokens.size() != 5) {
			printUsage(tokens[0]);
			return false;
		}
		wait(updateChangeFeed(
		    localDb, tokens[2], ChangeFeedStatus::CHANGE_FEED_CREATE, KeyRangeRef(tokens[3], tokens[4])));
	} else if (tokencmp(tokens[1], "stop")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return false;
		}
		wait(updateChangeFeed(localDb, tokens[2], ChangeFeedStatus::CHANGE_FEED_STOP));
	} else if (tokencmp(tokens[1], "destroy")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return false;
		}
		wait(updateChangeFeed(localDb, tokens[2], ChangeFeedStatus::CHANGE_FEED_DESTROY));
	} else if (tokencmp(tokens[1], "stream")) {
		if (tokens.size() < 3 || tokens.size() > 5) {
			printUsage(tokens[0]);
			return false;
		}
		Version begin = 0;
		Version end = std::numeric_limits<Version>::max();
		if (tokens.size() > 3) {
			int n = 0;
			if (sscanf(tokens[3].toString().c_str(), "%ld%n", &begin, &n) != 1 || n != tokens[3].size()) {
				printUsage(tokens[0]);
				return false;
			}
		}
		if (tokens.size() > 4) {
			int n = 0;
			if (sscanf(tokens[4].toString().c_str(), "%" PRId64 "%n", &end, &n) != 1 || n != tokens[4].size()) {
				printUsage(tokens[0]);
				return false;
			}
		}
		if (warn.isValid()) {
			warn.cancel();
		}
		state Reference<ChangeFeedData> feedData = makeReference<ChangeFeedData>();
		state Future<Void> feed = localDb->getChangeFeedStream(feedData, tokens[2], begin, end);
		state Future<Void> versionUpdates = requestVersionUpdate(localDb, feedData);
		printf("\n");
		try {
			state Future<Void> feedInterrupt = LineNoise::onKeyboardInterrupt();
			loop {
				choose {
					when(Standalone<VectorRef<MutationsAndVersionRef>> res =
					         waitNext(feedData->mutations.getFuture())) {
						for (auto& it : res) {
							for (auto& it2 : it.mutations) {
								fmt::print("{0} {1}\n", it.version, it2.toString());
							}
						}
					}
					when(wait(feedInterrupt)) {
						feedInterrupt = Future<Void>();
						feed.cancel();
						feedData = makeReference<ChangeFeedData>();
						break;
					}
				}
			}
			return true;
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				return true;
			}
			throw;
		}
	} else if (tokencmp(tokens[1], "pop")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return false;
		}
		Version v;
		int n = 0;
		if (sscanf(tokens[3].toString().c_str(), "%ld%n", &v, &n) != 1 || n != tokens[3].size()) {
			printUsage(tokens[0]);
			return false;
		} else {
			wait(localDb->popChangeFeedMutations(tokens[2], v));
		}
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory changeFeedFactory(
    "changefeed",
    CommandHelp("changefeed <register|destroy|stop|stream|pop|list> <RANGEID> <BEGIN> <END>", "", ""));
} // namespace fdb_cli
