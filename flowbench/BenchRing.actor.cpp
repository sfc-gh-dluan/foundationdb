#include "benchmark/benchmark.h"

#include <vector>
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<int> ring(Future<int> f) {
    int val = wait(f);
    return val + 1;
}

ACTOR static Future<Void> BM_RingActor(benchmark::State* benchState){

    while (benchState->KeepRunning()) {
        benchState->PauseTiming();

        std::vector<Future<int>> futures;
        futures.reserve(benchState->range(0));

        Promise<int> p;
        futures.push_back(ring(p.getFuture()));
        for (int i = 1; i < benchState->range(0); ++i) {
            futures.push_back(ring(futures.back()));
        }

        benchState->ResumeTiming();
        
        p.send(1);
        int result = wait(futures.back());
        
    }

    benchState->SetItemsProcessed(static_cast<long>(benchState->iterations()) * benchState->range(0));

    return Void();
}

static void bench_ring_flow(benchmark::State& benchState) {
    onMainThread([&benchState]() { return BM_RingActor(&benchState); }).blockUntilReady();
}

BENCHMARK(bench_ring_flow)->RangeMultiplier(2)->Range(8 << 2, 8 << 11);
