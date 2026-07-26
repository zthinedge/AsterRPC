#include "minirpc/cluster/Endpoint.h"
#include "minirpc/health/EndpointState.h"
#include "minirpc/loadbalance/P2cEwmaLoadBalancer.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

using namespace minirpc;

namespace{

using Microseconds=std::chrono::microseconds;
using EndpointSnapshot=
    loadbalance::P2cEwmaLoadBalancer::EndpointSnapshot;

EndpointSnapshot MakeSnapshot(
    std::initializer_list<cluster::Endpoint> endpoints
){
    return std::make_shared<const std::vector<cluster::Endpoint>>(
        endpoints
    );
}

void TestEndpointStateEwmaAndScore(){
    health::EndpointStateOptions options;
    options.weight=2.0;
    options.ewma_alpha=0.5;

    health::EndpointState state(
        cluster::Endpoint("127.0.0.1",9001),
        options
    );

    auto first=state.TryAcquire();
    assert(first==health::RequestKind::Normal);
    state.CompleteSuccess(Microseconds(100),*first);

    auto second=state.TryAcquire();
    assert(second==health::RequestKind::Normal);
    state.CompleteSuccess(Microseconds(300),*second);

    auto snapshot=state.Snapshot();
    assert(std::abs(snapshot.ewma_latency_us-200.0)<0.001);
    assert(snapshot.has_latency_sample);
    assert(std::abs(snapshot.score-100.0)<0.001);
    assert(snapshot.inflight==0);
    assert(snapshot.status==health::HealthStatus::Healthy);
}

void TestColdEndpointsAreExplored(){
    loadbalance::P2cEwmaLoadBalancer balancer(
        health::EndpointStateOptions{},
        5
    );
    auto endpoints=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002)
    });

    auto first=balancer.Select(endpoints);
    assert(first.has_value());
    cluster::Endpoint first_endpoint=first->GetEndpoint();
    first->CompleteSuccess(Microseconds(100));

    auto second=balancer.Select(endpoints);
    assert(second.has_value());
    assert(second->GetEndpoint()!=first_endpoint);
    second->CompleteSuccess(Microseconds(200));
}

void TestFailureHalfOpenAndRecovery(){
    health::EndpointStateOptions options;
    options.failure_threshold=2;
    options.cooldown=std::chrono::milliseconds(100);

    health::EndpointState state(
        cluster::Endpoint("127.0.0.1",9001),
        options
    );
    auto now=health::EndpointState::TimePoint{};

    auto first=state.TryAcquire(now);
    assert(first==health::RequestKind::Normal);
    state.CompleteFailure(Microseconds(100),*first,now);
    assert(state.Snapshot(now).status==
           health::HealthStatus::Suspect);
    assert(state.Snapshot(now).selectable);

    auto second=state.TryAcquire(now);
    assert(second==health::RequestKind::Normal);
    state.CompleteFailure(Microseconds(100),*second,now);

    auto unhealthy=state.Snapshot(now);
    assert(unhealthy.status==health::HealthStatus::Unhealthy);
    assert(!unhealthy.selectable);
    assert(!state.TryAcquire(
        now+std::chrono::milliseconds(99)
    ).has_value());

    auto probe=state.TryAcquire(
        now+std::chrono::milliseconds(100)
    );
    assert(probe==health::RequestKind::HalfOpenProbe);
    assert(!state.TryAcquire(
        now+std::chrono::milliseconds(100)
    ).has_value());

    state.CompleteFailure(
        Microseconds(50),
        *probe,
        now+std::chrono::milliseconds(100)
    );
    assert(state.Snapshot(
        now+std::chrono::milliseconds(150)
    ).status==health::HealthStatus::Unhealthy);

    auto recovery_probe=state.TryAcquire(
        now+std::chrono::milliseconds(200)
    );
    assert(recovery_probe==health::RequestKind::HalfOpenProbe);
    state.CompleteSuccess(
        Microseconds(50),
        *recovery_probe,
        now+std::chrono::milliseconds(200)
    );

    auto recovered=state.Snapshot(
        now+std::chrono::milliseconds(200)
    );
    assert(recovered.status==health::HealthStatus::Healthy);
    assert(recovered.consecutive_failures==0);
    assert(recovered.selectable);
}

void TestSelectionSpreadsInflight(){
    loadbalance::P2cEwmaLoadBalancer balancer(
        health::EndpointStateOptions{},
        7
    );
    auto endpoints=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002)
    });

    auto first=balancer.Select(endpoints);
    auto second=balancer.Select(endpoints);
    assert(first.has_value());
    assert(second.has_value());

    cluster::Endpoint first_endpoint=first->GetEndpoint();
    cluster::Endpoint second_endpoint=second->GetEndpoint();
    assert(first_endpoint!=second_endpoint);

    first->CompleteSuccess(Microseconds(100));
    second->CompleteSuccess(Microseconds(500));
    assert(first->GetEndpoint()==first_endpoint);
    assert(second->GetEndpoint()==second_endpoint);

    auto third=balancer.Select(endpoints);
    assert(third.has_value());
    assert(third->GetEndpoint()==first_endpoint);
    third->CompleteSuccess(Microseconds(100));
}

void TestWeightAffectsSelection(){
    loadbalance::P2cEwmaLoadBalancer balancer(
        health::EndpointStateOptions{},
        11
    );
    cluster::Endpoint high_weight("127.0.0.1",9001);
    cluster::Endpoint normal_weight("127.0.0.1",9002);
    auto endpoints=MakeSnapshot({high_weight,normal_weight});

    balancer.SetWeight(high_weight,4.0);
    auto selected=balancer.Select(endpoints);
    assert(selected.has_value());
    assert(selected->GetEndpoint()==high_weight);
    selected->CompleteSuccess(Microseconds(100));
}

void TestUnhealthyEndpointIsSkipped(){
    health::EndpointStateOptions options;
    options.failure_threshold=1;
    options.cooldown=std::chrono::hours(1);

    loadbalance::P2cEwmaLoadBalancer balancer(options,13);
    cluster::Endpoint failed("127.0.0.1",9001);
    cluster::Endpoint healthy("127.0.0.1",9002);
    auto endpoints=MakeSnapshot({failed,healthy});

    balancer.GetHealthChecker().Update(endpoints);
    auto failed_state=balancer.GetHealthChecker().Find(failed);
    assert(failed_state!=nullptr);

    auto request=failed_state->TryAcquire();
    assert(request.has_value());
    failed_state->CompleteFailure(Microseconds(100),*request);

    auto selected=balancer.Select(endpoints);
    assert(selected.has_value());
    assert(selected->GetEndpoint()==healthy);
    selected->CompleteSuccess(Microseconds(100));
}

void TestSnapshotChanges(){
    loadbalance::P2cEwmaLoadBalancer balancer(
        health::EndpointStateOptions{},
        17
    );
    auto two=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002)
    });
    auto one=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9002)
    });

    auto selected=balancer.Select(two);
    assert(selected.has_value());
    cluster::Endpoint selected_endpoint=selected->GetEndpoint();
    selected->Cancel();
    assert(balancer.GetHealthChecker().Size()==2);
    assert(
        balancer.GetHealthChecker().
            Find(selected_endpoint)->Snapshot().inflight==0
    );

    selected=balancer.Select(one);
    assert(selected.has_value());
    assert(selected->GetEndpoint().Port()==9002);
    selected->Cancel();
    assert(balancer.GetHealthChecker().Size()==1);

    assert(!balancer.Select(nullptr).has_value());
    assert(balancer.GetHealthChecker().Size()==0);
}

void TestConcurrentSelection(){
    loadbalance::P2cEwmaLoadBalancer balancer(
        health::EndpointStateOptions{},
        19
    );
    auto endpoints=MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9001),
        cluster::Endpoint("127.0.0.1",9002),
        cluster::Endpoint("127.0.0.1",9003)
    });

    constexpr std::size_t kThreads=8;
    constexpr std::size_t kCallsPerThread=2000;
    std::atomic_size_t completed{0};
    std::vector<std::thread> threads;

    for(std::size_t thread=0;thread<kThreads;++thread){
        threads.emplace_back([&](){
            for(std::size_t call=0;call<kCallsPerThread;++call){
                auto selected=balancer.Select(endpoints);
                assert(selected.has_value());
                selected->CompleteSuccess(Microseconds(100));
                completed.fetch_add(1,std::memory_order_relaxed);
            }
        });
    }

    for(auto& thread:threads){
        thread.join();
    }

    assert(completed.load(std::memory_order_relaxed)==
           kThreads*kCallsPerThread);

    for(const auto& endpoint:*endpoints){
        auto state=balancer.GetHealthChecker().Find(endpoint);
        assert(state!=nullptr);
        assert(state->Snapshot().inflight==0);
    }
}

void TestSlowEndpointReceivesLessTraffic(){
    health::EndpointStateOptions options;
    options.ewma_alpha=1.0;
    loadbalance::P2cEwmaLoadBalancer balancer(options,23);
    cluster::Endpoint fast("127.0.0.1",9001);
    cluster::Endpoint slow("127.0.0.1",9002);
    auto endpoints=MakeSnapshot({fast,slow});

    balancer.GetHealthChecker().Update(endpoints);
    auto fast_state=balancer.GetHealthChecker().Find(fast);
    auto slow_state=balancer.GetHealthChecker().Find(slow);

    auto kind=fast_state->TryAcquire();
    fast_state->CompleteSuccess(Microseconds(1000),*kind);
    kind=slow_state->TryAcquire();
    slow_state->CompleteSuccess(Microseconds(200000),*kind);

    std::size_t fast_requests=0;
    std::size_t slow_requests=0;
    for(std::size_t request=0;request<1000;++request){
        auto selection=balancer.Select(endpoints);
        assert(selection.has_value());

        if(selection->GetEndpoint()==fast){
            ++fast_requests;
            selection->CompleteSuccess(Microseconds(1000));
        }else{
            ++slow_requests;
            selection->CompleteSuccess(Microseconds(200000));
        }
    }

    assert(fast_requests>950);
    assert(slow_requests<50);

    options.ewma_alpha=0.5;
    options.failure_threshold=1;
    balancer.UpdateOptions(options);
    assert(balancer.GetHealthChecker().Find(fast)!=nullptr);
    assert(balancer.GetHealthChecker().Find(slow)!=nullptr);
}

}

int main(){
    TestEndpointStateEwmaAndScore();
    TestColdEndpointsAreExplored();
    TestFailureHalfOpenAndRecovery();
    TestSelectionSpreadsInflight();
    TestWeightAffectsSelection();
    TestUnhealthyEndpointIsSkipped();
    TestSnapshotChanges();
    TestConcurrentSelection();
    TestSlowEndpointReceivesLessTraffic();
    return 0;
}
