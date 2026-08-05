#include "asterrpc/cluster/Endpoint.h"
#include "asterrpc/cluster/RoundRobin.h"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

using namespace asterrpc;

namespace{

cluster::RoundRobin::EndpointSnapshot MakeSnapshot(
    std::initializer_list<cluster::Endpoint> endpoints
){
    return std::make_shared<const std::vector<cluster::Endpoint>>(
        endpoints
    );
}

void TestEmptySnapshot(){
    cluster::RoundRobin round_robin;

    assert(!round_robin.Select(nullptr).has_value());
    assert(!round_robin.Select(MakeSnapshot({})).has_value());
}

void TestRoundRobinOrder(){
    cluster::RoundRobin round_robin;
    auto endpoints=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });

    assert(round_robin.Select(endpoints)->Port()==9001);
    assert(round_robin.Select(endpoints)->Port()==9002);
    assert(round_robin.Select(endpoints)->Port()==9003);
    assert(round_robin.Select(endpoints)->Port()==9001);
}

void TestSnapshotChanges(){
    cluster::RoundRobin round_robin;
    auto three=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });

    for(int index=0;index<5;++index){
        assert(round_robin.Select(three).has_value());
    }

    auto one=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9002)
    });
    for(int index=0;index<20;++index){
        assert(round_robin.Select(one)->Port()==9002);
    }

    auto two=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });
    for(int index=0;index<20;++index){
        std::uint16_t port=round_robin.Select(two)->Port();
        assert(port==9002||port==9003);
    }
}

void TestConcurrentSelection(){
    cluster::RoundRobin round_robin;
    auto endpoints=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });

    constexpr std::size_t kThreads=12;
    constexpr std::size_t kSelectionsPerThread=3000;
    std::array<std::atomic_size_t,3> counts{};
    std::vector<std::thread> threads;

    for(std::size_t thread=0;thread<kThreads;++thread){
        threads.emplace_back([&](){
            for(std::size_t index=0;
                index<kSelectionsPerThread;
                ++index){
                std::uint16_t port=
                    round_robin.Select(endpoints)->Port();
                counts[port-9001].fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
        });
    }

    for(auto& thread:threads){
        thread.join();
    }

    std::size_t expected=
        kThreads*kSelectionsPerThread/counts.size();
    for(const auto& count:counts){
        assert(count.load(std::memory_order_relaxed)==expected);
    }
}

void TestConcurrentSnapshotChanges(){
    cluster::RoundRobin round_robin;
    auto one=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001)
    });
    auto three=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });
    cluster::RoundRobin::EndpointSnapshot current=one;

    constexpr std::size_t kWorkers=8;
    constexpr std::size_t kSelectionsPerWorker=10000;
    std::atomic_bool start{false};
    std::vector<std::thread> threads;

    threads.emplace_back([&](){
        while(!start.load(std::memory_order_acquire)){
        }

        for(std::size_t index=0;
            index<kSelectionsPerWorker;
            ++index){
            std::atomic_store_explicit(
                &current,
                index%2==0?three:one,
                std::memory_order_release
            );
        }
    });

    for(std::size_t worker=0;worker<kWorkers;++worker){
        threads.emplace_back([&](){
            while(!start.load(std::memory_order_acquire)){
            }

            for(std::size_t index=0;
                index<kSelectionsPerWorker;
                ++index){
                auto snapshot=std::atomic_load_explicit(
                    &current,
                    std::memory_order_acquire
                );
                auto selected=round_robin.Select(snapshot);
                assert(selected.has_value());
                assert(selected->Port()>=9001);
                assert(selected->Port()<=9003);
            }
        });
    }

    start.store(true,std::memory_order_release);
    for(auto& thread:threads){
        thread.join();
    }
}

}

int main(){
    TestEmptySnapshot();
    TestRoundRobinOrder();
    TestSnapshotChanges();
    TestConcurrentSelection();
    TestConcurrentSnapshotChanges();
    return 0;
}
