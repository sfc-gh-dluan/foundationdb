/*
 * genericactors.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"
#include "flow/cpp20coro.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/genericactors.actor.h"
#include <ostream>
#include <sstream>
#include <string>

ACTOR Future<bool> allTrue(std::vector<Future<bool>> all) {
	state int i = 0;
	while (i != all.size()) {
		bool r = wait(all[i]);
		if (!r)
			return false;
		i++;
	}
	return true;
}

ACTOR Future<Void> anyTrue(std::vector<Reference<AsyncVar<bool>>> input, Reference<AsyncVar<bool>> output) {
	loop {
		bool oneTrue = false;
		std::vector<Future<Void>> changes;
		for (auto it : input) {
			if (it->get())
				oneTrue = true;
			changes.push_back(it->onChange());
		}
		output->set(oneTrue);
		wait(waitForAny(changes));
	}
}

ACTOR Future<Void> cancelOnly(std::vector<Future<Void>> futures) {
	// We don't do anything with futures except hold them, we never return, but if we are cancelled we (naturally) drop
	// the futures
	wait(Never());
	return Void();
}

ACTOR Future<Void> timeoutWarningCollector(FutureStream<Void> input, double logDelay, const char* context, UID id) {
	state uint64_t counter = 0;
	state Future<Void> end = delay(logDelay);
	loop choose {
		when(waitNext(input)) { counter++; }
		when(wait(end)) {
			if (counter)
				TraceEvent(SevWarn, context, id).detail("LateProcessCount", counter).detail("LoggingDelay", logDelay);
			end = delay(logDelay);
			counter = 0;
		}
	}
}

ACTOR Future<Void> waitForMost(std::vector<Future<ErrorOr<Void>>> futures,
                               int faultTolerance,
                               Error e,
                               double waitMultiplierForSlowFutures) {
	state std::vector<Future<bool>> successFutures;
	state double startTime = now();
	successFutures.reserve(futures.size());
	for (const auto& future : futures) {
		successFutures.push_back(fmap([](auto const& result) { return result.present(); }, future));
	}
	bool success = wait(quorumEqualsTrue(successFutures, successFutures.size() - faultTolerance));
	if (!success) {
		throw e;
	}
	wait(delay((now() - startTime) * waitMultiplierForSlowFutures) || waitForAll(successFutures));
	return Void();
}

ACTOR Future<bool> quorumEqualsTrue(std::vector<Future<bool>> futures, int required) {
	state std::vector<Future<Void>> true_futures;
	state std::vector<Future<Void>> false_futures;
	true_futures.reserve(futures.size());
	false_futures.reserve(futures.size());
	for (int i = 0; i < futures.size(); i++) {
		true_futures.push_back(onEqual(futures[i], true));
		false_futures.push_back(onEqual(futures[i], false));
	}

	choose {
		when(wait(quorum(true_futures, required))) { return true; }
		when(wait(quorum(false_futures, futures.size() - required + 1))) { return false; }
	}
}

ACTOR Future<bool> shortCircuitAny(std::vector<Future<bool>> f) {
	std::vector<Future<Void>> sc;
	sc.reserve(f.size());
	for (Future<bool> fut : f) {
		sc.push_back(returnIfTrue(fut));
	}

	choose {
		when(wait(waitForAll(f))) {
			// Handle a possible race condition? If the _last_ term to
			// be evaluated triggers the waitForAll before bubbling
			// out of the returnIfTrue quorum
			for (const auto& fut : f) {
				if (fut.get()) {
					return true;
				}
			}
			return false;
		}
		when(wait(waitForAny(sc))) { return true; }
	}
}

Future<Void> orYield(Future<Void> f) {
	if (f.isReady()) {
		if (f.isError())
			return tagError<Void>(yield(), f.getError());
		else
			return yield();
	} else
		return f;
}

ACTOR Future<Void> returnIfTrue(Future<bool> f) {
	bool b = wait(f);
	if (b) {
		return Void();
	}
	wait(Never());
	throw internal_error();
}

ACTOR Future<Void> lowPriorityDelay(double waitTime) {
	state int loopCount = 0;
	state int totalLoops =
	    std::max<int>(waitTime / FLOW_KNOBS->LOW_PRIORITY_MAX_DELAY, FLOW_KNOBS->LOW_PRIORITY_DELAY_COUNT);

	while (loopCount < totalLoops) {
		wait(delay(waitTime / totalLoops, TaskPriority::Low));
		loopCount++;
	}
	return Void();
}

namespace {

struct DummyState {
	int changed{ 0 };
	int unchanged{ 0 };
	bool operator==(DummyState const& rhs) const { return changed == rhs.changed && unchanged == rhs.unchanged; }
	bool operator!=(DummyState const& rhs) const { return !(*this == rhs); }
};

ACTOR Future<Void> testPublisher(Reference<AsyncVar<DummyState>> input) {
	state int i = 0;
	for (; i < 100; ++i) {
		wait(delay(deterministicRandom()->random01()));
		auto var = input->get();
		++var.changed;
		input->set(var);
	}
	return Void();
}

ACTOR Future<Void> testSubscriber(Reference<IAsyncListener<int>> output, Optional<int> expected) {
	loop {
		wait(output->onChange());
		ASSERT(expected.present());
		if (output->get() == expected.get()) {
			return Void();
		}
	}
}

static Future<ErrorOr<Void>> goodTestFuture(double duration) {
	return tag(delay(duration), ErrorOr<Void>(Void()));
}

static Future<ErrorOr<Void>> badTestFuture(double duration, Error e) {
	return tag(delay(duration), ErrorOr<Void>(e));
}

} // namespace

TEST_CASE("/flow/genericactors/AsyncListener") {
	auto input = makeReference<AsyncVar<DummyState>>();
	state Future<Void> subscriber1 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.changed; }), 100);
	state Future<Void> subscriber2 =
	    testSubscriber(IAsyncListener<int>::create(input, [](auto const& var) { return var.unchanged; }), {});
	wait(subscriber1 && testPublisher(input));
	ASSERT(!subscriber2.isReady());
	return Void();
}

TEST_CASE("/flow/genericactors/WaitForMost") {
	state std::vector<Future<ErrorOr<Void>>> futures;
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 1, operation_failed(), 0.0)); // Don't wait for slowest future
		ASSERT(!futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 0, operation_failed(), 0.0)); // Wait for all futures
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), goodTestFuture(3) };
		wait(waitForMost(futures, 1, operation_failed(), 1.0)); // Wait for slowest future
		ASSERT(futures[2].isReady());
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		wait(waitForMost(futures, 1, operation_failed(), 1.0)); // Error ignored
	}
	{
		futures = { goodTestFuture(1), goodTestFuture(2), badTestFuture(1, success()) };
		try {
			wait(waitForMost(futures, 0, operation_failed(), 1.0));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	return Void();
}

template <class T>
Future<T> timeoutCoro(Future<T> what, double time, T timedoutValue, TaskPriority taskID = TaskPriority::DefaultDelay) {
	Future<Void> end = delay(time, taskID);

	if (what.isReady()) {
		co_return what.get();
	}

	co_await(ready(what) || end);

	// std::cout << "  co_awaited " << what.isReady() << " " << end.isReady() << std::endl;

	if (what.isReady()) {
		// std::cout << "  what is ready " << what.isError() << (what.isError() ? what.getError().what() : "")
		//           << std::endl;
		co_return what.get();
	} else {
		co_return timedoutValue;
	}
}

ACTOR template <class T>
Future<T> timeoutFlow(Future<T> what, double time, T timedoutValue, TaskPriority taskID = TaskPriority::DefaultDelay) {
	Future<Void> end = delay(time, taskID);

	choose {
		when(T t = wait(what)) { return t; }
		when(wait(end)) { return timedoutValue; }
	}
}

ACTOR Future<Void> long_delay() {
	wait(delay(1000));
	return Void();
}

TEST_CASE("flow/coroutine/timeout") {
	// Ready future
	state Future<int> f1 = Future<int>(1);
	int v = wait(timeoutFlow(f1, 1, -1));
	ASSERT(v == 1);

	// future timed out
	state Promise<int> p;
	state Future<int> f2 = timeoutFlow(p.getFuture(), 1, -1);
	wait(delay(2));
	int v2 = wait(f2);
	ASSERT(v2 == -1);

	// future become ready
	state Promise<int> p2;
	Future<int> f3 = timeoutCoro(p2.getFuture(), 1, -1);
	// p2.send(1);
	p2.sendError(io_error());
	try {
		int v3 = wait(f3);
	} catch (Error& e) {
		std::cout << e.what() << std::endl;
	}
	// ASSERT(v3 == 1);

	// Error future
	state Future<int> f4 = Future<int>(io_error());
	state Future<int> f5 = timeoutFlow(f4, 1, -1);
	wait(ready(f5));
	ASSERT(f5.isReady() && f5.isError() && f5.getError().code() == error_code_io_error);

	// Cancelled future
	state Future<Void> f6 = long_delay();
	state Future<Void> f7 = timeoutFlow(f6, 1, Void());
	f6.cancel();
	wait(ready(f7));
	ASSERT(f7.isReady() && f7.isError() && f7.getError().code() == error_code_actor_cancelled);

	return Void();
}

struct LifetimeLogger {
	LifetimeLogger(std::ostream& ss, int id) : ss(ss), id(id) { ss << "LifetimeLogger(" << id << "). "; }
	~LifetimeLogger() { ss << "~LifetimeLogger(" << id << "). "; }

	std::ostream& ss;
	int id;
};

template <typename T>
Future<Void> simple_await_test(std::stringstream& ss, Future<T> f) {
	ss << "start. ";

	LifetimeLogger ll1(ss, 0);

	co_await(f);
	ss << "wait returned. ";

	LifetimeLogger ll2(ss, 1);

	co_return Void();
	ss << "after co_return. ";
}

Future<Void> actor_cancel_test(std::stringstream& ss) {
	ss << "start. ";

	LifetimeLogger ll(ss, 0);

	try {
		co_await(delay(100));
	} catch (Error& e) {
		ss << "error: " << e.what() << ". ";
	}

	ss << "wait returned. ";

	co_return Void();
	ss << "after co_return. ";
}


Future<Void> cancel_actor(Future<Void>&& f, int d) {
	Future<Void> fc = std::move(f);

	co_await(delay(d) || fc);
	std::cout << "after delay" << std::endl;

	co_return Void();
}

Future<Void> actor_throw_test(std::stringstream& ss) {
	ss << "start. ";

	LifetimeLogger ll(ss, 0);

	throw io_error();

	ss << "after throw. ";
	co_return Void();
	ss << "after co_return. ";
}

TEST_CASE("flow/coroutine/actor") {

	{
		state std::stringstream ss1;
		try {
			wait(simple_await_test(ss1, delay(1)));
		} catch (Error& e) {
			ss1 << "error: " << e.what() << ". ";
		}
		std::cout << ss1.str() << std::endl;
		ASSERT(ss1.str() == "start. LifetimeLogger(0). wait returned. LifetimeLogger(1). ~LifetimeLogger(1). "
		                    "~LifetimeLogger(0). ");
	}

	{
		state std::stringstream ss2;
		try {
			wait(simple_await_test(ss2, Future<int>(io_error())));
		} catch (Error& e) {
			ss2 << "error: " << e.what() << ". ";
		}
		std::cout << ss2.str() << std::endl;
		ASSERT(ss2.str() == "start. LifetimeLogger(0). ~LifetimeLogger(0). error: Disk i/o operation failed. ");
	}

	{
		state std::stringstream ss3;
		{
			Future<Void> f = actor_cancel_test(ss3);
			wait(delay(1));
		}
		std::cout << ss3.str() << std::endl;
		ASSERT(ss3.str() == "start. LifetimeLogger(0). error: Asynchronous operation cancelled. wait returned. "
		                    "~LifetimeLogger(0). ");
	}

	{
		state std::stringstream ss4;
		try {
			wait(actor_throw_test(ss4));
		} catch (Error& e) {
			ss4 << "error: " << e.what() << ". ";
		}
		std::cout << ss4.str() << std::endl;
		ASSERT(ss4.str() == "start. LifetimeLogger(0). ~LifetimeLogger(0). error: Disk i/o operation failed. ");
	}

	return Void();
}

#if false
TEST_CASE("/flow/genericactors/generic/storeTuple") {
	state std::vector<UID> resA;
	state int resB;
	state double resC;

	state Promise<std::tuple<std::vector<UID>, int, double>> promise;

	auto future = storeTuple(promise.getFuture(), resA, resB, resC);

	promise.send(std::make_tuple(std::vector<UID>(10), 15, 2.0));
	wait(ready(future));
	ASSERT(resA.size() == 10);
	ASSERT(resB == 15);
	ASSERT(resC == 2.0);
	return Void();
}
#endif
