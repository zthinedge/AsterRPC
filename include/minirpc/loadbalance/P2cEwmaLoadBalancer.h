#pragma once

#include "minirpc/cluster/Endpoint.h"
#include "minirpc/health/EndpointState.h"
#include "minirpc/health/HealthChecker.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <vector>

namespace minirpc::loadbalance{

class P2cEwmaLoadBalancer{
public:
    using EndpointSnapshot=
        std::shared_ptr<const std::vector<cluster::Endpoint>>;

    class Selection{
    public:
        Selection(Selection&& other)noexcept;
        Selection& operator=(Selection&& other)noexcept;
        ~Selection();

        Selection(const Selection&)=delete;
        Selection& operator=(const Selection&)=delete;

        const cluster::Endpoint& GetEndpoint()const noexcept;

        void CompleteSuccess(std::chrono::microseconds latency);
        void CompleteFailure(std::chrono::microseconds latency);
        void Cancel()noexcept;

    private:
        friend class P2cEwmaLoadBalancer;

        Selection(
            std::shared_ptr<health::EndpointState> state,
            health::RequestKind kind
        );

        cluster::Endpoint endpoint_;
        std::shared_ptr<health::EndpointState> state_;
        health::RequestKind kind_=health::RequestKind::Normal;
    };

    explicit P2cEwmaLoadBalancer(
        health::EndpointStateOptions options={}
    );

    P2cEwmaLoadBalancer(
        health::EndpointStateOptions options,
        std::uint64_t random_seed
    );

    std::optional<Selection> Select(
        const EndpointSnapshot& endpoints
    );

    void SetWeight(
        const cluster::Endpoint& endpoint,
        double weight
    );

    health::HealthChecker& GetHealthChecker()noexcept;
    const health::HealthChecker& GetHealthChecker()const noexcept;

private:
    using StatePtr=std::shared_ptr<health::EndpointState>;

    std::optional<Selection> Acquire(const StatePtr& state);

    std::pair<StatePtr,StatePtr> PickTwo(
        const std::vector<StatePtr>& states
    );

    health::HealthChecker health_checker_;
    std::mutex random_mutex_;
    std::mt19937_64 random_;
};

}
