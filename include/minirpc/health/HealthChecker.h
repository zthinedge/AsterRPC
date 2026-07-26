#pragma once

#include "minirpc/cluster/Endpoint.h"
#include "minirpc/health/EndpointState.h"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace minirpc::health{

class HealthChecker{
public:
    using EndpointSnapshot=
        std::shared_ptr<const std::vector<cluster::Endpoint>>;
    using StateList=std::vector<std::shared_ptr<EndpointState>>;
    using StateSnapshot=std::shared_ptr<const StateList>;

    explicit HealthChecker(EndpointStateOptions options={});

    StateSnapshot Update(const EndpointSnapshot& endpoints);

    std::shared_ptr<EndpointState> Find(
        const cluster::Endpoint& endpoint
    )const;

    void SetWeight(
        const cluster::Endpoint& endpoint,
        double weight
    );

    std::size_t Size()const;

private:
    EndpointStateOptions options_;
    mutable std::mutex mutex_;
    std::unordered_map<
        cluster::Endpoint,
        std::shared_ptr<EndpointState>,
        cluster::EndpointHash
    > states_;
    std::unordered_map<
        cluster::Endpoint,
        double,
        cluster::EndpointHash
    > weights_;
    EndpointSnapshot endpoint_snapshot_;
    StateSnapshot state_snapshot_;
};

}
