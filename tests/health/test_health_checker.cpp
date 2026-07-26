#include "minirpc/cluster/Endpoint.h"
#include "minirpc/health/EndpointState.h"
#include "minirpc/health/HealthChecker.h"
#include "minirpc/net/EventLoop.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using namespace minirpc;

namespace{

class LoopThread{
public:
    LoopThread(){
        std::promise<net::EventLoop*> ready;
        auto future=ready.get_future();

        thread_=std::thread([ready=std::move(ready)]()mutable{
            net::EventLoop loop;
            ready.set_value(&loop);
            loop.Loop();
        });
        loop_=future.get();
    }

    ~LoopThread(){
        loop_->Stop();
        thread_.join();
    }

    LoopThread(const LoopThread&)=delete;
    LoopThread& operator=(const LoopThread&)=delete;

    net::EventLoop* Loop()const noexcept{
        return loop_;
    }

private:
    net::EventLoop* loop_=nullptr;
    std::thread thread_;
};

bool WaitUntil(
    const std::function<bool()>& predicate,
    std::chrono::milliseconds timeout
){
    auto deadline=std::chrono::steady_clock::now()+timeout;
    while(std::chrono::steady_clock::now()<deadline){
        if(predicate()){
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

health::HealthChecker::EndpointSnapshot MakeSnapshot(
    std::initializer_list<cluster::Endpoint> endpoints
){
    return std::make_shared<const std::vector<cluster::Endpoint>>(
        endpoints
    );
}

void TestSuspectState(){
    health::EndpointStateOptions options;
    options.failure_threshold=2;
    health::EndpointState state(
        cluster::Endpoint("127.0.0.1",9001),
        options
    );

    assert(state.TryBeginHealthCheck());
    state.CompleteHealthCheck(false);

    auto suspect=state.Snapshot();
    assert(suspect.status==health::HealthStatus::Suspect);
    assert(suspect.selectable);
    assert(suspect.consecutive_failures==1);
    assert(suspect.failure_penalty_us>0.0);

    assert(state.TryBeginHealthCheck());
    state.CompleteHealthCheck(true);

    auto healthy=state.Snapshot();
    assert(healthy.status==health::HealthStatus::Healthy);
    assert(healthy.consecutive_failures==0);
}

void TestPeriodicTimeoutAndRecovery(){
    LoopThread loop_thread;

    health::EndpointStateOptions state_options;
    state_options.failure_threshold=2;
    state_options.cooldown=std::chrono::milliseconds(100);

    health::HealthChecker checker(state_options);
    cluster::Endpoint endpoint("127.0.0.1",9001);
    checker.Update(MakeSnapshot({endpoint}));

    std::atomic_int mode{0};
    std::atomic_size_t probes{0};

    health::ActiveHealthCheckOptions active_options;
    active_options.interval=std::chrono::milliseconds(10);
    active_options.timeout=std::chrono::milliseconds(5);

    checker.Start(
        loop_thread.Loop(),
        [&mode,&probes](
            const cluster::Endpoint&,
            std::chrono::milliseconds,
            health::HealthChecker::ProbeCompletion completion
        ){
            probes.fetch_add(1,std::memory_order_relaxed);
            int current=mode.load(std::memory_order_relaxed);
            if(current==1){
                completion(false);
            }else if(current==2){
                completion(true);
            }
        },
        active_options
    );

    assert(checker.IsRunning());
    assert(WaitUntil(
        [&checker,&endpoint](){
            auto state=checker.Find(endpoint);
            return state!=nullptr&&
                   state->Snapshot().status==
                       health::HealthStatus::Unhealthy;
        },
        std::chrono::seconds(1)
    ));
    assert(probes.load(std::memory_order_relaxed)>=2);

    mode.store(2,std::memory_order_relaxed);
    assert(WaitUntil(
        [&checker,&endpoint](){
            auto state=checker.Find(endpoint);
            return state!=nullptr&&
                   state->Snapshot().status==
                       health::HealthStatus::Healthy;
        },
        std::chrono::seconds(1)
    ));

    checker.Stop();
    assert(!checker.IsRunning());

    std::size_t stopped_count=probes.load(std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    assert(probes.load(std::memory_order_relaxed)==stopped_count);
}

void TestHotOptionUpdates(){
    health::EndpointStateOptions state_options;
    state_options.failure_threshold=3;
    state_options.ewma_alpha=0.2;

    health::EndpointState state(
        cluster::Endpoint("127.0.0.1",9001),
        state_options
    );
    auto request=state.TryAcquire();
    state.CompleteFailure(
        std::chrono::microseconds(100),
        *request
    );
    assert(state.Snapshot().status==
           health::HealthStatus::Suspect);

    state_options.failure_threshold=1;
    state_options.ewma_alpha=1.0;
    state.UpdateOptions(state_options);
    assert(state.Snapshot().status==
           health::HealthStatus::Unhealthy);

    health::EndpointState latency_state(
        cluster::Endpoint("127.0.0.1",9002)
    );
    request=latency_state.TryAcquire();
    latency_state.CompleteSuccess(
        std::chrono::microseconds(100),
        *request
    );

    health::EndpointStateOptions latency_options;
    latency_options.ewma_alpha=1.0;
    latency_state.UpdateOptions(latency_options);
    request=latency_state.TryAcquire();
    latency_state.CompleteSuccess(
        std::chrono::microseconds(400),
        *request
    );
    assert(latency_state.Snapshot().ewma_latency_us==400.0);

    LoopThread loop_thread;
    health::HealthChecker checker;
    checker.Update(MakeSnapshot({
        cluster::Endpoint("127.0.0.1",9003)
    }));
    std::atomic_size_t probes{0};

    health::ActiveHealthCheckOptions active_options;
    active_options.interval=std::chrono::seconds(1);
    active_options.timeout=std::chrono::milliseconds(50);
    checker.Start(
        loop_thread.Loop(),
        [&probes](
            const cluster::Endpoint&,
            std::chrono::milliseconds,
            health::HealthChecker::ProbeCompletion completion
        ){
            probes.fetch_add(1,std::memory_order_relaxed);
            completion(true);
        },
        active_options
    );
    assert(WaitUntil(
        [&probes](){
            return probes.load(std::memory_order_relaxed)>=1;
        },
        std::chrono::milliseconds(100)
    ));

    active_options.interval=std::chrono::milliseconds(10);
    checker.UpdateActiveOptions(active_options);
    assert(WaitUntil(
        [&probes](){
            return probes.load(std::memory_order_relaxed)>=3;
        },
        std::chrono::milliseconds(150)
    ));
    checker.Stop();
}

}

int main(){
    TestSuspectState();
    TestPeriodicTimeoutAndRecovery();
    TestHotOptionUpdates();
    return 0;
}
