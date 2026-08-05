#include "asterrpc/cluster/RetryPolicy.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace asterrpc::cluster{
namespace{

double RandomUnit()noexcept{
    static std::atomic_uint64_t state{
        static_cast<std::uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count()
        )
    };

    std::uint64_t value=state.fetch_add(
        0x9e3779b97f4a7c15ULL,
        std::memory_order_relaxed
    );
    value=(value^(value>>30))*0xbf58476d1ce4e5b9ULL;
    value=(value^(value>>27))*0x94d049bb133111ebULL;
    value^=value>>31;

    constexpr double denominator=
        static_cast<double>(std::uint64_t{1}<<53);
    return static_cast<double>(value>>11)/denominator;
}

}

RetryPolicy::RetryPolicy(
    std::size_t max_attempts,
    std::chrono::microseconds initial_backoff,
    double backoff_multiplier,
    std::chrono::microseconds max_backoff,
    double jitter_ratio
):max_attempts_(max_attempts),
  initial_backoff_(initial_backoff),
  backoff_multiplier_(backoff_multiplier),
  max_backoff_(max_backoff),
  jitter_ratio_(jitter_ratio){
    if(max_attempts_==0){
        throw std::invalid_argument("retry max attempts must be positive");
    }
    if(initial_backoff_<std::chrono::microseconds::zero()||
       max_backoff_<std::chrono::microseconds::zero()){
        throw std::invalid_argument("retry backoff must not be negative");
    }
    if(!std::isfinite(backoff_multiplier_)||
       backoff_multiplier_<1.0){
        throw std::invalid_argument(
            "retry backoff multiplier must be at least one"
        );
    }
    if(!std::isfinite(jitter_ratio_)||
       jitter_ratio_<0.0||jitter_ratio_>1.0){
        throw std::invalid_argument(
            "retry jitter ratio must be between zero and one"
        );
    }
}

std::size_t RetryPolicy::MaxAttempts()const noexcept{
    return max_attempts_;
}

bool RetryPolicy::ShouldRetry(
    protocol::StatusCode status,
    std::size_t attempts_completed
)const noexcept{
    bool retryable=
        status==protocol::StatusCode::ConnectionFailed||
        status==protocol::StatusCode::InternalError;
    return retryable&&attempts_completed<max_attempts_;
}

std::chrono::microseconds RetryPolicy::BackoffForRetry(
    std::size_t attempts_completed
)const noexcept{
    if(attempts_completed==0||initial_backoff_.count()==0){
        return std::chrono::microseconds::zero();
    }

    long double delay=static_cast<long double>(
        initial_backoff_.count()
    );
    for(std::size_t attempt=1;attempt<attempts_completed;++attempt){
        delay*=backoff_multiplier_;
        if(delay>=static_cast<long double>(max_backoff_.count())){
            delay=static_cast<long double>(max_backoff_.count());
            break;
        }
    }

    delay=std::min(
        delay,
        static_cast<long double>(max_backoff_.count())
    );
    delay=std::min(
        delay,
        static_cast<long double>(
            std::numeric_limits<std::chrono::microseconds::rep>::max()
        )
    );

    if(jitter_ratio_>0.0&&delay>0.0L){
        double jitter=(RandomUnit()*2.0-1.0)*jitter_ratio_;
        delay*=1.0+jitter;
        delay=std::max(delay,0.0L);
        delay=std::min(
            delay,
            static_cast<long double>(max_backoff_.count())
        );
    }

    return std::chrono::microseconds(
        static_cast<std::chrono::microseconds::rep>(delay)
    );
}

}
