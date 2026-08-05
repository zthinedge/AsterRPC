#pragma once

#include "asterrpc/protocol/RpcMeta.h"

#include <chrono>
#include <cstddef>

namespace asterrpc::cluster{

class RetryPolicy{
public:
    RetryPolicy(
        std::size_t max_attempts=1,
        std::chrono::microseconds initial_backoff=
            std::chrono::microseconds::zero(),
        double backoff_multiplier=2.0,
        std::chrono::microseconds max_backoff=
            std::chrono::seconds(1),
        double jitter_ratio=0.2
    );

    std::size_t MaxAttempts()const noexcept;

    bool ShouldRetry(
        protocol::StatusCode status,
        std::size_t attempts_completed
    )const noexcept;

    std::chrono::microseconds BackoffForRetry(
        std::size_t attempts_completed
    )const noexcept;

private:
    std::size_t max_attempts_;
    std::chrono::microseconds initial_backoff_;
    double backoff_multiplier_;
    std::chrono::microseconds max_backoff_;
    double jitter_ratio_;
};

}
