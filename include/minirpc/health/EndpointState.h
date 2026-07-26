#pragma once

#include "minirpc/cluster/Endpoint.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>

namespace minirpc::health{

enum class HealthStatus{
    Healthy,
    Unhealthy,
    HalfOpen
};

enum class RequestKind{
    Normal,
    HalfOpenProbe
};

struct EndpointStateOptions{
    double weight=1.0;
    double ewma_alpha=0.2;
    std::chrono::microseconds initial_latency{1000};
    std::chrono::microseconds failure_penalty{5000};
    std::size_t failure_threshold=3;
    std::chrono::milliseconds cooldown{5000};
};

struct EndpointStateSnapshot{
    HealthStatus status=HealthStatus::Healthy;
    double ewma_latency_us=0.0;
    std::uint64_t inflight=0;
    std::size_t consecutive_failures=0;
    double weight=1.0;
    double failure_penalty_us=0.0;
    double score=0.0;
    bool selectable=false;
};

class EndpointState{
public:
    using Clock=std::chrono::steady_clock;
    using TimePoint=Clock::time_point;

    EndpointState(
        cluster::Endpoint endpoint,
        EndpointStateOptions options={}
    );

    const cluster::Endpoint& GetEndpoint()const noexcept;

    std::optional<RequestKind> TryAcquire(
        TimePoint now=Clock::now()
    );

    void CompleteSuccess(
        std::chrono::microseconds latency,
        RequestKind kind,
        TimePoint now=Clock::now()
    );

    void CompleteFailure(
        std::chrono::microseconds latency,
        RequestKind kind,
        TimePoint now=Clock::now()
    );

    void Cancel(RequestKind kind)noexcept;

    EndpointStateSnapshot Snapshot(
        TimePoint now=Clock::now()
    );

    void SetWeight(double weight);

private:
    void RefreshHealth(TimePoint now)noexcept;
    void UpdateEwma(std::chrono::microseconds latency)noexcept;
    void DecreaseInflight()noexcept;
    void MarkUnhealthy(TimePoint now)noexcept;

    cluster::Endpoint endpoint_;
    EndpointStateOptions options_;

    mutable std::mutex mutex_;
    HealthStatus status_=HealthStatus::Healthy;
    double ewma_latency_us_=0.0;
    bool has_latency_sample_=false;
    std::uint64_t inflight_=0;
    std::size_t consecutive_failures_=0;
    TimePoint retry_at_{};
    bool half_open_probe_inflight_=false;
};

}
