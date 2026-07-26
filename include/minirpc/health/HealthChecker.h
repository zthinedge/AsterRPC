#pragma once

#include "minirpc/cluster/Endpoint.h"
#include "minirpc/health/EndpointState.h"

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

namespace minirpc::net{
class EventLoop;
}

namespace minirpc::health{

struct ActiveHealthCheckOptions{
    std::chrono::milliseconds interval{2000};
    std::chrono::milliseconds timeout{1000};
};

class HealthChecker{
public:
    using EndpointSnapshot=
        std::shared_ptr<const std::vector<cluster::Endpoint>>;
    using StateList=std::vector<std::shared_ptr<EndpointState>>;
    using StateSnapshot=std::shared_ptr<const StateList>;
    using ProbeCompletion=std::function<void(bool)>;
    using ProbeFunction=std::function<void(
        const cluster::Endpoint&,
        std::chrono::milliseconds,
        ProbeCompletion
    )>;

    explicit HealthChecker(EndpointStateOptions options={});
    ~HealthChecker();

    HealthChecker(const HealthChecker&)=delete;
    HealthChecker& operator=(const HealthChecker&)=delete;

    StateSnapshot Update(const EndpointSnapshot& endpoints);

    std::shared_ptr<EndpointState> Find(
        const cluster::Endpoint& endpoint
    )const;

    void SetWeight(
        const cluster::Endpoint& endpoint,
        double weight
    );

    std::size_t Size()const;

    void Start(
        net::EventLoop* loop,
        ProbeFunction probe,
        ActiveHealthCheckOptions options={}
    );

    void Stop()noexcept;
    bool IsRunning()const noexcept;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}
