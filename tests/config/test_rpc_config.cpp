#include "asterrpc/config/RpcConfig.h"

#include <cassert>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

using namespace asterrpc;

namespace{

void TestJsonParsingAndValidation(){
    config::RpcConfigPatch patch=config::ParseRpcConfigPatch(R"({
        "default_timeout_ms": 800,
        "retry_count": 2,
        "load_balancer": "round_robin",
        "health_check_interval_ms": 500,
        "failure_threshold": 5,
        "ewma_alpha": 0.35
    })");

    config::RpcConfig merged=config::Merge(
        config::RpcConfig{},
        patch
    );
    assert(merged.default_timeout==std::chrono::milliseconds(800));
    assert(merged.retry_count==2);
    assert(merged.load_balancer==
           config::LoadBalancerAlgorithm::RoundRobin);
    assert(merged.health_check_interval==
           std::chrono::milliseconds(500));
    assert(merged.failure_threshold==5);
    assert(merged.ewma_alpha==0.35);

    bool rejected=false;
    try{
        config::ParseRpcConfigPatch(R"({"retry_cout": 2})");
    }catch(const std::invalid_argument&){
        rejected=true;
    }
    assert(rejected);

    rejected=false;
    try{
        config::ParseRpcConfigPatch(R"({"ewma_alpha": 2.0})");
    }catch(const std::invalid_argument&){
        rejected=true;
    }
    assert(rejected);
}

void TestGlobalAndServiceOverrides(){
    config::ConfigStore store;
    store.UpdateGlobal(config::ParseRpcConfigPatch(R"({
        "default_timeout_ms": 1000,
        "retry_count": 1,
        "load_balancer": "p2c_ewma"
    })"));
    store.UpdateService(
        "UserService",
        config::ParseRpcConfigPatch(R"({
            "default_timeout_ms": 200,
            "retry_count": 3
        })")
    );

    auto user=store.Get("UserService");
    assert(user->default_timeout==std::chrono::milliseconds(200));
    assert(user->retry_count==3);
    assert(user->load_balancer==
           config::LoadBalancerAlgorithm::P2cEwma);

    auto order=store.Get("OrderService");
    assert(order->default_timeout==
           std::chrono::milliseconds(1000));
    assert(order->retry_count==1);

    store.UpdateGlobal(config::ParseRpcConfigPatch(R"({
        "default_timeout_ms": 1500,
        "retry_count": 2,
        "load_balancer": "round_robin"
    })"));

    user=store.Get("UserService");
    assert(user->default_timeout==std::chrono::milliseconds(200));
    assert(user->retry_count==3);
    assert(user->load_balancer==
           config::LoadBalancerAlgorithm::RoundRobin);

    order=store.Get("OrderService");
    assert(order->default_timeout==
           std::chrono::milliseconds(1500));
    assert(order->retry_count==2);
}

void TestSubscribersReceiveImmutableSnapshots(){
    config::ConfigStore store;
    std::vector<config::ConfigStore::Snapshot> snapshots;
    auto listener=store.Subscribe(
        "UserService",
        [&snapshots](config::ConfigStore::Snapshot snapshot){
            snapshots.push_back(std::move(snapshot));
        }
    );

    assert(snapshots.size()==1);
    auto initial=snapshots.front();

    store.UpdateGlobal(config::ParseRpcConfigPatch(
        R"({"default_timeout_ms": 900})"
    ));
    store.UpdateService(
        "UserService",
        config::ParseRpcConfigPatch(
            R"({"default_timeout_ms": 250})"
        )
    );

    assert(snapshots.size()==3);
    assert(initial->default_timeout==
           std::chrono::milliseconds(1200));
    assert(snapshots[1]->default_timeout==
           std::chrono::milliseconds(900));
    assert(snapshots[2]->default_timeout==
           std::chrono::milliseconds(250));
    assert(initial!=snapshots.back());

    store.Unsubscribe(listener);
    store.UpdateService("UserService",{});
    assert(snapshots.size()==3);
}

}

int main(){
    TestJsonParsingAndValidation();
    TestGlobalAndServiceOverrides();
    TestSubscribersReceiveImmutableSnapshots();
    return 0;
}
