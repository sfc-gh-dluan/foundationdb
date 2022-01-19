/*
 * ClientLibManagementWorkload.actor.cpp
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

#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ClientLibManagement.actor.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "fdbclient/md5/md5.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace ClientLibManagement;

/**
 * Workload for testing ClientLib management operations, declared in
 * MultiVersionClientControl.actor.h
 */
struct ClientLibManagementWorkload : public TestWorkload {
	static constexpr size_t FILE_CHUNK_SIZE = 128 * 1024; // Used for test setup only

	size_t testFileSize = 0;
	RandomByteGenerator rbg;
	Standalone<StringRef> uploadedClientLibId;
	json_spirit::mObject uploadedMetadataJson;
	Standalone<StringRef> generatedChecksum;
	std::string generatedFileName;
	bool success = true;

	/*----------------------------------------------------------------
	 *  Interface
	 */

	ClientLibManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		int minTestFileSize = getOption(options, LiteralStringRef("minTestFileSize"), 0);
		int maxTestFileSize = getOption(options, LiteralStringRef("maxTestFileSize"), 1024 * 1024);
		testFileSize = deterministicRandom()->randomInt(minTestFileSize, maxTestFileSize + 1);
	}

	std::string description() const override { return "ClientLibManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(this); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	/*----------------------------------------------------------------
	 *  Setup
	 */

	ACTOR Future<Void> _setup(ClientLibManagementWorkload* self) {
		state Reference<AsyncFileBuffer> data = self->allocateBuffer(FILE_CHUNK_SIZE);
		state size_t fileOffset;
		state MD5_CTX sum;
		state size_t bytesToWrite;

		self->generatedFileName = format("clientLibUpload%d", self->clientId);
		int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE |
		                IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
		state Reference<IAsyncFile> file =
		    wait(IAsyncFileSystem::filesystem()->open(self->generatedFileName, flags, 0666));

		::MD5_Init(&sum);

		for (fileOffset = 0; fileOffset < self->testFileSize; fileOffset += FILE_CHUNK_SIZE) {
			self->rbg.writeRandomBytesToBuffer(data->buffer, FILE_CHUNK_SIZE);
			bytesToWrite = std::min(FILE_CHUNK_SIZE, self->testFileSize - fileOffset);
			wait(file->write(data->buffer, bytesToWrite, fileOffset));

			::MD5_Update(&sum, data->buffer, bytesToWrite);
		}
		wait(file->sync());

		self->generatedChecksum = md5SumToHexString(sum);

		return Void();
	}

	/*----------------------------------------------------------------
	 *  Tests
	 */

	ACTOR static Future<Void> _start(ClientLibManagementWorkload* self, Database cx) {
		wait(testUploadClientLibInvalidInput(self, cx));
		wait(testClientLibUploadFileDoesNotExist(self, cx));
		wait(testUploadClientLib(self, cx));
		wait(testClientLibListAfterUpload(self, cx));
		wait(testDownloadClientLib(self, cx));
		wait(testClientLibDownloadNotExisting(self, cx));
		wait(testChangeClientLibStatusErrors(self, cx));
		wait(testDisableClientLib(self, cx));
		wait(testChangeStateToDownload(self, cx));
		wait(testDeleteClientLib(self, cx));
		wait(testUploadedClientLibInList(self, cx, ClientLibFilter(), false, "No filter, after delete"));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLibInvalidInput(ClientLibManagementWorkload* self, Database cx) {
		state std::vector<std::string> invalidMetadataStrs = {
			"{foo", // invalid json
			"[]", // json array
		};
		state StringRef metadataStr;

		// add garbage attribute
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		metadataJson["unknownattr"] = "someval";
		invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));

		const std::string mandatoryAttrs[] = { CLIENTLIB_ATTR_PLATFORM,    CLIENTLIB_ATTR_VERSION,
			                                   CLIENTLIB_ATTR_CHECKSUM,    CLIENTLIB_ATTR_TYPE,
			                                   CLIENTLIB_ATTR_GIT_HASH,    CLIENTLIB_ATTR_PROTOCOL,
			                                   CLIENTLIB_ATTR_API_VERSION, CLIENTLIB_ATTR_CHECKSUM_ALG };

		for (const std::string& attr : mandatoryAttrs) {
			validClientLibMetadataSample(metadataJson);
			metadataJson.erase(attr);
			invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		}

		for (auto& testMetadataStr : invalidMetadataStrs) {
			metadataStr = StringRef(testMetadataStr);
			wait(testExpectedError(uploadClientLibrary(cx, metadataStr, StringRef(self->generatedFileName)),
			                       "uploadClientLibrary with invalid metadata",
			                       client_lib_invalid_metadata(),
			                       &self->success,
			                       { { "Metadata", metadataStr.toString().c_str() } }));
		}

		return Void();
	}

	ACTOR static Future<Void> testClientLibUploadFileDoesNotExist(ClientLibManagementWorkload* self, Database cx) {
		state Standalone<StringRef> metadataStr;
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		wait(testExpectedError(uploadClientLibrary(cx, metadataStr, "some_not_existing_file_name"_sr),
		                       "uploadClientLibrary with a not existing file",
		                       file_not_found(),
		                       &self->success));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLibWrongChecksum(ClientLibManagementWorkload* self, Database cx) {
		state Standalone<StringRef> metadataStr;
		validClientLibMetadataSample(self->uploadedMetadataJson);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(self->uploadedMetadataJson)));
		self->uploadedClientLibId = getClientLibIdFromMetadataJson(metadataStr);
		wait(testExpectedError(uploadClientLibrary(cx, metadataStr, StringRef(self->generatedFileName)),
		                       "uploadClientLibrary wrong checksum",
		                       client_lib_invalid_binary(),
		                       &self->success));
		wait(testUploadedClientLibInList(self, cx, ClientLibFilter(), false, "After upload with wrong checksum"));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLib(ClientLibManagementWorkload* self, Database cx) {
		state Standalone<StringRef> metadataStr;
		state std::vector<Future<ErrorOr<Void>>> concurrentUploads;
		state Future<Void> clientLibChanged = cx->onClientLibStatusChanged();

		validClientLibMetadataSample(self->uploadedMetadataJson);
		self->uploadedMetadataJson[CLIENTLIB_ATTR_CHECKSUM] = self->generatedChecksum.toString();
		// avoid clientLibId clashes, when multiple clients try to upload the same file
		self->uploadedMetadataJson[CLIENTLIB_ATTR_TYPE] = format("devbuild%d", self->clientId);
		self->uploadedMetadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(ClientLibStatus::ACTIVE);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(self->uploadedMetadataJson)));
		self->uploadedClientLibId = getClientLibIdFromMetadataJson(metadataStr);

		// Test two concurrent uploads of the same library, one of the must fail and another succeed
		for (int i1 = 0; i1 < 2; i1++) {
			Future<Void> uploadActor = uploadClientLibrary(cx, metadataStr, StringRef(self->generatedFileName));
			concurrentUploads.push_back(errorOr(uploadActor));
		}

		wait(waitForAll(concurrentUploads));

		int successCnt = 0;
		for (auto uploadRes : concurrentUploads) {
			if (uploadRes.get().isError()) {
				self->testErrorCode(
				    "concurrent client lib upload", client_lib_already_exists(), uploadRes.get().getError());
			} else {
				successCnt++;
			}
		}

		if (successCnt == 0) {
			TraceEvent(SevError, "ClientLibUploadFailed").log();
			self->success = false;
			throw operation_failed();
		} else if (successCnt > 1) {
			TraceEvent(SevError, "ClientLibConflictingUpload").log();
			self->success = false;
		}

		// Clients should be notified about upload of a library with the active status
		Optional<Void> notificationWait = wait(timeout(clientLibChanged, 100.0));
		if (!notificationWait.present()) {
			TraceEvent(SevError, "ClientLibChangeNotificationFailed").log();
			self->success = false;
		}

		return Void();
	}

	ACTOR static Future<Void> testClientLibDownloadNotExisting(ClientLibManagementWorkload* self, Database cx) {
		// Generate a random valid clientLibId
		state Standalone<StringRef> clientLibId;
		state std::string destFileName;
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		Standalone<StringRef> metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		clientLibId = getClientLibIdFromMetadataJson(metadataStr);

		destFileName = format("clientLibDownload%d", self->clientId);
		wait(testExpectedError(downloadClientLibrary(cx, StringRef(clientLibId), StringRef(destFileName)),
		                       "download not existing client library",
		                       client_lib_not_found(),
		                       &self->success));
		return Void();
	}

	ACTOR static Future<Void> testDownloadClientLib(ClientLibManagementWorkload* self, Database cx) {
		state std::string destFileName = format("clientLibDownload%d", self->clientId);
		wait(downloadClientLibrary(cx, self->uploadedClientLibId, StringRef(destFileName)));

		FILE* f = fopen(destFileName.c_str(), "r");
		if (f == nullptr) {
			TraceEvent(SevError, "ClientLibDownloadFileDoesNotExist").detail("FileName", destFileName);
			self->success = false;
		} else {
			fseek(f, 0L, SEEK_END);
			size_t fileSize = ftell(f);
			if (fileSize != self->testFileSize) {
				TraceEvent(SevError, "ClientLibDownloadFileSizeMismatch")
				    .detail("ExpectedSize", self->testFileSize)
				    .detail("ActualSize", fileSize);
				self->success = false;
			}
			fclose(f);
		}

		return Void();
	}

	ACTOR static Future<Void> testDeleteClientLib(ClientLibManagementWorkload* self, Database cx) {
		state Future<Void> clientLibChanged = cx->onClientLibStatusChanged();

		wait(deleteClientLibrary(cx, self->uploadedClientLibId));

		// Clients should be notified about deletion of the library, because it has "download" status
		Optional<Void> notificationWait = wait(timeout(clientLibChanged, 100.0));
		if (!notificationWait.present()) {
			TraceEvent(SevError, "ClientLibChangeNotificationFailed").log();
		}
		return Void();
	}

	ACTOR static Future<Void> testClientLibListAfterUpload(ClientLibManagementWorkload* self, Database cx) {
		state int uploadedApiVersion = self->uploadedMetadataJson[CLIENTLIB_ATTR_API_VERSION].get_int();
		state ClientLibPlatform uploadedPlatform =
		    getPlatformByName(self->uploadedMetadataJson[CLIENTLIB_ATTR_PLATFORM].get_str());
		state std::string uploadedVersion = self->uploadedMetadataJson[CLIENTLIB_ATTR_VERSION].get_str();
		state ClientLibFilter filter;

		filter = ClientLibFilter();
		wait(testUploadedClientLibInList(self, cx, filter, true, "No filter"));
		filter = ClientLibFilter().filterAvailable();
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter available"));
		filter = ClientLibFilter().filterAvailable().filterCompatibleAPI(uploadedApiVersion);
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter available, the same API"));
		filter = ClientLibFilter().filterAvailable().filterCompatibleAPI(uploadedApiVersion + 1);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, newer API"));
		filter = ClientLibFilter().filterCompatibleAPI(uploadedApiVersion).filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter the same API, the same platform"));
		ASSERT(uploadedPlatform != ClientLibPlatform::X86_64_WINDOWS);
		filter = ClientLibFilter().filterAvailable().filterPlatform(ClientLibPlatform::X86_64_WINDOWS);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, different platform"));
		filter = ClientLibFilter().filterAvailable().filterNewerPackageVersion(uploadedVersion);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, the same version"));
		filter =
		    ClientLibFilter().filterAvailable().filterNewerPackageVersion("1.15.10").filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(
		    self, cx, filter, true, "Filter available, an older version, the same platform"));
		filter = ClientLibFilter()
		             .filterAvailable()
		             .filterNewerPackageVersion(uploadedVersion)
		             .filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(
		    self, cx, filter, false, "Filter available, the same version, the same platform"));
		filter = ClientLibFilter().filterNewerPackageVersion("100.1.1");
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter a newer version"));
		filter = ClientLibFilter().filterNewerPackageVersion("1.15.10");
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter an older version"));
		return Void();
	}

	ACTOR static Future<Void> testUploadedClientLibInList(ClientLibManagementWorkload* self,
	                                                      Database cx,
	                                                      ClientLibFilter filter,
	                                                      bool expectInList,
	                                                      const char* testDescr) {
		Standalone<VectorRef<StringRef>> allLibs = wait(listClientLibraries(cx, filter));
		bool found = false;
		for (StringRef metadataJson : allLibs) {
			Standalone<StringRef> clientLibId;
			clientLibId = getClientLibIdFromMetadataJson(metadataJson);
			if (clientLibId == self->uploadedClientLibId) {
				found = true;
			}
		}
		if (found != expectInList) {
			TraceEvent(SevError, "ClientLibInListTestFailed")
			    .detail("Test", testDescr)
			    .detail("ClientLibId", self->uploadedClientLibId)
			    .detail("Expected", expectInList)
			    .detail("Actual", found);
			self->success = false;
		}
		return Void();
	}

	ACTOR static Future<Void> testChangeClientLibStatusErrors(ClientLibManagementWorkload* self, Database cx) {
		wait(testExpectedError(changeClientLibraryStatus(cx, self->uploadedClientLibId, ClientLibStatus::UPLOADING),
		                       "Setting invalid client library status",
		                       client_lib_invalid_metadata(),
		                       &self->success));

		wait(testExpectedError(changeClientLibraryStatus(cx, "notExistingClientLib"_sr, ClientLibStatus::DOWNLOAD),
		                       "Changing not existing client library status",
		                       client_lib_not_found(),
		                       &self->success));
		return Void();
	}

	ACTOR static Future<Void> testDisableClientLib(ClientLibManagementWorkload* self, Database cx) {
		state std::string destFileName = format("clientLibDownload%d", self->clientId);
		state Future<Void> clientLibChanged = cx->onClientLibStatusChanged();

		// Set disabled status on the uploaded library
		wait(changeClientLibraryStatus(cx, self->uploadedClientLibId, ClientLibStatus::DISABLED));
		state ClientLibStatus newStatus = wait(getClientLibraryStatus(cx, self->uploadedClientLibId));
		if (newStatus != ClientLibStatus::DISABLED) {
			TraceEvent(SevError, "ClientLibDisableClientLibFailed")
			    .detail("Reason", "Unexpected status")
			    .detail("Expected", ClientLibStatus::DISABLED)
			    .detail("Actual", newStatus);
			self->success = false;
		}

		// Clients should be notified about an active library being disabled
		Optional<Void> notificationWait = wait(timeout(clientLibChanged, 100.0));
		if (!notificationWait.present()) {
			TraceEvent(SevError, "ClientLibChangeNotificationFailed").log();
			self->success = false;
		}

		// It should not be possible to download a disabled client library
		wait(testExpectedError(downloadClientLibrary(cx, self->uploadedClientLibId, StringRef(destFileName)),
		                       "Downloading disabled client library",
		                       client_lib_not_available(),
		                       &self->success));

		return Void();
	}

	ACTOR static Future<Void> testChangeStateToDownload(ClientLibManagementWorkload* self, Database cx) {
		state std::string destFileName = format("clientLibDownload%d", self->clientId);
		state Future<Void> clientLibChanged = cx->onClientLibStatusChanged();

		// Set disabled status on the uploaded library
		wait(changeClientLibraryStatus(cx, self->uploadedClientLibId, ClientLibStatus::DOWNLOAD));
		state ClientLibStatus newStatus = wait(getClientLibraryStatus(cx, self->uploadedClientLibId));
		if (newStatus != ClientLibStatus::DOWNLOAD) {
			TraceEvent(SevError, "ClientLibChangeStatusFailed")
			    .detail("Reason", "Unexpected status")
			    .detail("Expected", ClientLibStatus::DOWNLOAD)
			    .detail("Actual", newStatus);
			self->success = false;
		}

		Optional<Void> notificationWait = wait(timeout(clientLibChanged, 100.0));
		if (!notificationWait.present()) {
			TraceEvent(SevError, "ClientLibChangeNotificationFailed").log();
			self->success = false;
		}

		return Void();
	}

	/* ----------------------------------------------------------------
	 * Utility methods
	 */

	Reference<AsyncFileBuffer> allocateBuffer(size_t size) { return makeReference<AsyncFileBuffer>(size, false); }

	static std::string randomHexadecimalStr(int length) {
		std::string s;
		s.reserve(length);
		for (int i = 0; i < length; i++) {
			uint32_t hexDigit = static_cast<char>(deterministicRandom()->randomUInt32() % 16);
			char ch = (hexDigit >= 10 ? hexDigit - 10 + 'a' : hexDigit + '0');
			s += ch;
		}
		return s;
	}

	static void validClientLibMetadataSample(json_spirit::mObject& metadataJson) {
		metadataJson.clear();
		metadataJson[CLIENTLIB_ATTR_PLATFORM] = getPlatformName(ClientLibPlatform::X86_64_LINUX);
		metadataJson[CLIENTLIB_ATTR_VERSION] = "7.1.0";
		metadataJson[CLIENTLIB_ATTR_GIT_HASH] = randomHexadecimalStr(40);
		metadataJson[CLIENTLIB_ATTR_TYPE] = "debug";
		metadataJson[CLIENTLIB_ATTR_CHECKSUM] = randomHexadecimalStr(32);
		metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(ClientLibStatus::DOWNLOAD);
		metadataJson[CLIENTLIB_ATTR_API_VERSION] = 710;
		metadataJson[CLIENTLIB_ATTR_PROTOCOL] = "fdb00b07001001";
		metadataJson[CLIENTLIB_ATTR_CHECKSUM_ALG] = "md5";
	}

	void testErrorCode(const char* testDescr,
	                   Error expectedError,
	                   Error actualError,
	                   std::map<std::string, std::string> details = {},
	                   UID id = UID()) {
		ASSERT(expectedError.isValid());
		ASSERT(actualError.isValid());
		if (expectedError.code() != actualError.code()) {
			TraceEvent evt(SevError, "TestErrorCodeFailed", id);
			evt.detail("TestDescription", testDescr);
			evt.detail("ExpectedError", expectedError.code());
			evt.error(actualError);
			for (auto& p : details) {
				evt.detail(p.first.c_str(), p.second);
			}
			success = false;
		}
	}
};

WorkloadFactory<ClientLibManagementWorkload> ClientLibOperationsWorkloadFactory("ClientLibManagement");
