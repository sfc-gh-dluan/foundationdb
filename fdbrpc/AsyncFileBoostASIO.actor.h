/*
 * AsyncFileBoostASIO.actor.h
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

#pragma once

#include <boost/version.hpp>

#if defined(__linux__) && defined(BOOST_ASIO_HAS_FILE)

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_AsyncFileBoostASIO_ACTOR_G_H)
#define FLOW_AsyncFileBoostASIO_ACTOR_G_H
#include "fdbrpc/AsyncFileBoostASIO.actor.g.h"
#elif !defined(FLOW_AsyncFileBoostASIO_ACTOR_H)
#define FLOW_AsyncFileBoostASIO_ACTOR_H

#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>

#include "fdbrpc/IAsyncFile.h"

#include <boost/bind/bind.hpp>
#include <boost/asio.hpp>
#include "boost/asio/random_access_file.hpp"

#include "flow/actorcompiler.h" // This must be the last #include.

class AsyncFileBoostASIO final : public IAsyncFile, public ReferenceCounted<AsyncFileBoostASIO> {
public:
	static void init() {}

	static void stop() {}

	static bool should_poll() { return false; }
	// FIXME: This implementation isn't actually asynchronous - it just does operations synchronously!

	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode, boost::asio::io_service* ios) {
		// ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		// ASSERT(flags & OPEN_UNBUFFERED);

		if (flags & OPEN_LOCK)
			mode |= 02000; // Enable mandatory locking for this file if it is supported by the filesystem

		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE));
			open_filename = filename + ".part";
		}

		int fd = ::open(open_filename.c_str(), openFlags(flags), mode);
		if (fd < 0) {
			Error e = errno == ENOENT ? file_not_found() : io_error();
			int ecode = errno; // Save errno in case it is modified before it is used below
			TraceEvent ev("AsyncFileBoostASIOOpenFailed");
			ev.error(e)
			    .detail("Filename", filename)
			    .detailf("Flags", "%x", flags)
			    .detailf("OSFlags", "%x", openFlags(flags))
			    .detailf("Mode", "0%o", mode)
			    .GetLastError();
			if (ecode == EINVAL)
				ev.detail("Description", "Invalid argument - Does the target filesystem support KAIO?");
			return e;
		} else {
			TraceEvent("AsyncFileBoostASIOOpen")
			    .detail("Filename", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode)
			    .detail("Fd", fd);
		}

		Reference<AsyncFileBoostASIO> r(new AsyncFileBoostASIO(*ios, fd, flags, filename));

		if (flags & OPEN_LOCK) {
			// Acquire a "write" lock for the entire file
			flock lockDesc;
			lockDesc.l_type = F_WRLCK;
			lockDesc.l_whence = SEEK_SET;
			lockDesc.l_start = 0;
			lockDesc.l_len =
			    0; // "Specifying 0 for l_len has the special meaning: lock all bytes starting at the location specified
			       // by l_whence and l_start through to the end of file, no matter how large the file grows."
			lockDesc.l_pid = 0;
			if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
				TraceEvent(SevError, "UnableToLockFile").detail("Filename", filename).GetLastError();
				return io_error();
			}
		}
		// struct stat buf;
		// if (fstat(fd, &buf)) {
		// 	TraceEvent("AsyncFileBoostASIOFStatError").detail("Fd", fd).detail("Filename", filename).GetLastError();
		// 	return io_error();
		// }

		// r->lastFileSize = r->nextFileSize = buf.st_size;
		return Reference<IAsyncFile>(std::move(r));
	}

	static int openFlags(int flags) {
		int oflags = O_DIRECT | O_CLOEXEC;
		ASSERT(bool(flags & OPEN_READONLY) != bool(flags & OPEN_READWRITE)); // readonly xor readwrite
		if (flags & OPEN_EXCLUSIVE)
			oflags |= O_EXCL;
		if (flags & OPEN_CREATE)
			oflags |= O_CREAT;
		if (flags & OPEN_READONLY)
			oflags |= O_RDONLY;
		if (flags & OPEN_READWRITE)
			oflags |= O_RDWR;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE)
			oflags |= O_TRUNC;
		return oflags;
	}

	void addref() override { ReferenceCounted<AsyncFileBoostASIO>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileBoostASIO>::delref(); }

	int64_t debugFD() const override { return (int64_t)(const_cast<decltype(file)&>(file).native_handle()); }

	static void onReadReady(Promise<int> onReady, const boost::system::error_code& error, size_t bytesRead) {
		if (error) {
			Error e = io_error();
			TraceEvent("AsyncReadError")
			    .error(e)
			    .GetLastError()
			    .detail("ASIOCode", error.value())
			    .detail("ASIOMessage", error.message());
			onReady.sendError(e);
		} else {
			onReady.send(bytesRead);
		}
	}
	static void onWriteReady(Promise<Void> onReady,
	                         size_t bytesExpected,
	                         const boost::system::error_code& error,
	                         size_t bytesWritten) {
		if (error) {
			Error e = io_error();
			TraceEvent("AsyncWriteError")
			    .error(e)
			    .GetLastError()
			    .detail("ASIOCode", error.value())
			    .detail("ASIOMessage", error.message());
			onReady.sendError(e);
		} else if (bytesWritten != bytesExpected) {
			Error e = io_error();
			TraceEvent("AsyncWriteError").detail("BytesExpected", bytesExpected).detail("BytesWritten", bytesWritten);
			onReady.sendError(io_error());
		} else {
			onReady.send(Void());
		}
	}

	Future<int> read(void* data, int length, int64_t offset) override {
		// the size call is set inline
		auto end = this->size().get();
		//TraceEvent("WinAsyncRead").detail("Offset", offset).detail("Length", length).detail("FileSize", end).detail("FileName", filename);
		if (offset >= end)
			return 0;

		Promise<int> result;
		file.async_read_some_at(
		    offset,
		    boost::asio::mutable_buffers_1(data, length),
		    boost::bind(
		        &onReadReady, result, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));

		return result.getFuture();
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		/*
		FIXME
		if ( length + offset >= fileValidData ) {
		    SetFileValidData( length+offset );
		    fileValidData = length+offset;
		}*/
		Promise<Void> result;
		boost::asio::async_write_at(file,
		                            offset,
		                            boost::asio::const_buffers_1(data, length),
		                            boost::bind(&onWriteReady,
		                                        result,
		                                        length,
		                                        boost::asio::placeholders::error,
		                                        boost::asio::placeholders::bytes_transferred));
		return result.getFuture();
	}
	Future<Void> truncate(int64_t size) override {
		// FIXME: Possibly use SetFileInformationByHandle( file.native_handle(), FileEndOfFileInfo, ... ) instead
		// if (!SetFilePointerEx(file.native_handle(), *(LARGE_INTEGER*)&size, nullptr, FILE_BEGIN))
		// 	throw io_error();
		// if (!SetEndOfFile(file.native_handle()))
		// 	throw io_error();

		return Void();
	}
	Future<Void> sync() override {
		// FIXME: Do FlushFileBuffers in a worker thread (using g_network->createThreadPool)?
		// if (!FlushFileBuffers(file.native_handle()))
		// 	throw io_error();

		// if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
		// 	flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;
		// 	// FIXME: MoveFileEx(..., MOVEFILE_WRITE_THROUGH) in thread?
		// 	MoveFile((filename + ".part").c_str(), filename.c_str());
		// }

		file.sync_all();
		// auto fsync = AsyncFileEIO::async_fdatasync(fd);

		// if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
		// 	flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;

		// 	return AsyncFileEIO::waitAndAtomicRename(fsync, filename + ".part", filename);
		// }

		// return fsync;
		return Void();
	}
	Future<int64_t> size() const override {
		// LARGE_INTEGER s;
		// if (!GetFileSizeEx(const_cast<decltype(file)&>(file).native_handle(), &s))
		// 	throw io_error();
		// return *(int64_t*)&s;

		return file.size();
	}
	std::string getFilename() const override { return filename; }

	~AsyncFileBoostASIO() {}

private:
	boost::asio::random_access_file file;
	int fd, flags;
	std::string filename;

	AsyncFileBoostASIO(boost::asio::io_service& ios, int fd, int flags, std::string filename)
	  : file(ios, fd), fd(fd), flags(flags), filename(filename) {}
};

ACTOR Future<double> writeBlock(int i, Reference<IAsyncFile> f, void* buf) {

	state double startTime = now();

	wait(f->write(buf, 4096, i * 4096));

	return now() - startTime;
}

TEST_CASE("/fdbrpc/AsyncFileBoostASIO/CallbackTest") {

	state bool use_io_uring = params.getInt("use_io_uring").orDefault(1);
	state int num_blocks = params.getInt("num_blocks").orDefault(1000);
	state std::string file_path = params.get("file_path").orDefault("");

	printf("use_io_uring: %d\n", use_io_uring);
	printf("num_blocks: %d\n", num_blocks);
	printf("file_path: %s\n", file_path.c_str());

	ASSERT(!file_path.empty());

	state Reference<IAsyncFile> f;
	try {
		if (use_io_uring) {

			Reference<IAsyncFile> f_ = wait(AsyncFileBoostASIO::open(
			    file_path,
			    IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
			    0666,
			    static_cast<boost::asio::io_service*>((void*)g_network->global(INetwork::enASIOService))));
			f = f_;

		} else {

			Reference<IAsyncFile> f_ = wait(AsyncFileKAIO::open(
			    file_path,
			    IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
			    0666,
			    static_cast<boost::asio::io_service*>((void*)g_network->global(INetwork::enASIOService))));
			f = f_;
		}

		state void* buf = FastAllocator<4096>::allocate();

		state std::vector<Future<double>> futures;
		for (int i = 0; i < num_blocks; ++i) {
			futures.push_back(writeBlock(i, f, buf));
		}

		FastAllocator<4096>::release(buf);

		state double sum = 0.0;
		state int i = 0;
		for (; i < num_blocks; ++i) {
			double time = wait(futures.at(i));
			sum += time;
		}

		printf("avg: %f\n", sum / num_blocks);

	} catch (Error& e) {
		state Error err = e;
		if (f) {
			wait(AsyncFileEIO::deleteFile(f->getFilename(), true));
		}

		throw err;
	}

	wait(AsyncFileEIO::deleteFile(f->getFilename(), true));

	return Void();
}

#include "flow/unactorcompiler.h"
#endif
#endif
