#include "minirpc/health/EndpointState.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace minirpc::health{

namespace{

void ValidateOptions(const EndpointStateOptions& options){
    if(!std::isfinite(options.weight)||options.weight<=0.0){
        throw std::invalid_argument("endpoint weight must be positive");
    }
    if(!std::isfinite(options.ewma_alpha)||
       options.ewma_alpha<=0.0||
       options.ewma_alpha>1.0){
        throw std::invalid_argument(
            "EWMA alpha must be in the range (0, 1]"
        );
    }
    if(options.initial_latency.count()<0){
        throw std::invalid_argument(
            "initial latency must not be negative"
        );
    }
    if(options.failure_penalty.count()<0){
        throw std::invalid_argument(
            "failure penalty must not be negative"
        );
    }
    if(options.failure_threshold==0){
        throw std::invalid_argument(
            "failure threshold must be positive"
        );
    }
    if(options.cooldown.count()<0){
        throw std::invalid_argument(
            "health cooldown must not be negative"
        );
    }
}

double ToMicros(std::chrono::microseconds latency)noexcept{
    return static_cast<double>(std::max<std::int64_t>(
        latency.count(),
        0
    ));
}

}

EndpointState::EndpointState(
    cluster::Endpoint endpoint,
    EndpointStateOptions options
):endpoint_(std::move(endpoint)),
  options_(options),
  ewma_latency_us_(ToMicros(options.initial_latency)){
    ValidateOptions(options_);
}

const cluster::Endpoint& EndpointState::GetEndpoint()const noexcept{
    return endpoint_;
}

std::optional<RequestKind> EndpointState::TryAcquire(TimePoint now){
    std::lock_guard<std::mutex> lock(mutex_);
    RefreshHealth(now);

    if(status_==HealthStatus::Unhealthy){
        return std::nullopt;
    }

    if(status_==HealthStatus::HalfOpen){
        if(half_open_probe_inflight_){
            return std::nullopt;
        }

        half_open_probe_inflight_=true;
        ++inflight_;
        return RequestKind::HalfOpenProbe;
    }

    ++inflight_;
    return RequestKind::Normal;
}

void EndpointState::CompleteSuccess(
    std::chrono::microseconds latency,
    RequestKind kind,
    TimePoint
){
    std::lock_guard<std::mutex> lock(mutex_);
    DecreaseInflight();
    UpdateEwma(latency);

    if(kind==RequestKind::HalfOpenProbe){
        half_open_probe_inflight_=false;
        status_=HealthStatus::Healthy;
        consecutive_failures_=0;
        retry_at_=TimePoint{};
        return;
    }

    if(status_==HealthStatus::Healthy||
       status_==HealthStatus::Suspect){
        status_=HealthStatus::Healthy;
        consecutive_failures_=0;
    }
}

void EndpointState::CompleteFailure(
    std::chrono::microseconds latency,
    RequestKind kind,
    TimePoint now
){
    std::lock_guard<std::mutex> lock(mutex_);
    DecreaseInflight();
    UpdateEwma(latency);

    if(kind==RequestKind::HalfOpenProbe){
        half_open_probe_inflight_=false;
        ++consecutive_failures_;
        MarkUnhealthy(now);
        return;
    }

    if(status_!=HealthStatus::Healthy&&
       status_!=HealthStatus::Suspect){
        return;
    }

    ++consecutive_failures_;
    if(consecutive_failures_>=options_.failure_threshold){
        MarkUnhealthy(now);
    }else{
        status_=HealthStatus::Suspect;
    }
}

void EndpointState::Cancel(RequestKind kind)noexcept{
    std::lock_guard<std::mutex> lock(mutex_);
    DecreaseInflight();

    if(kind==RequestKind::HalfOpenProbe){
        half_open_probe_inflight_=false;
    }
}

bool EndpointState::TryBeginHealthCheck(TimePoint now){
    std::lock_guard<std::mutex> lock(mutex_);
    RefreshHealth(now);

    if(status_==HealthStatus::Unhealthy||
       health_check_inflight_){
        return false;
    }

    if(status_==HealthStatus::HalfOpen){
        if(half_open_probe_inflight_){
            return false;
        }
        half_open_probe_inflight_=true;
        health_check_is_half_open_=true;
    }else{
        health_check_is_half_open_=false;
    }

    health_check_inflight_=true;
    return true;
}

void EndpointState::CompleteHealthCheck(
    bool success,
    TimePoint now
){
    std::lock_guard<std::mutex> lock(mutex_);
    if(!health_check_inflight_){
        return;
    }

    bool was_half_open=health_check_is_half_open_;
    health_check_inflight_=false;
    health_check_is_half_open_=false;

    if(was_half_open){
        half_open_probe_inflight_=false;
        if(success){
            status_=HealthStatus::Healthy;
            consecutive_failures_=0;
            retry_at_=TimePoint{};
        }else{
            ++consecutive_failures_;
            MarkUnhealthy(now);
        }
        return;
    }

    if(status_!=HealthStatus::Healthy&&
       status_!=HealthStatus::Suspect){
        return;
    }

    if(success){
        status_=HealthStatus::Healthy;
        consecutive_failures_=0;
        return;
    }

    ++consecutive_failures_;
    if(consecutive_failures_>=options_.failure_threshold){
        MarkUnhealthy(now);
    }else{
        status_=HealthStatus::Suspect;
    }
}

EndpointStateSnapshot EndpointState::Snapshot(TimePoint now){
    std::lock_guard<std::mutex> lock(mutex_);
    RefreshHealth(now);

    double penalty=
        static_cast<double>(options_.failure_penalty.count())*
        static_cast<double>(consecutive_failures_);
    double score=
        ewma_latency_us_*
        static_cast<double>(inflight_+1)/
        options_.weight+
        penalty;

    EndpointStateSnapshot snapshot;
    snapshot.status=status_;
    snapshot.ewma_latency_us=ewma_latency_us_;
    snapshot.inflight=inflight_;
    snapshot.consecutive_failures=consecutive_failures_;
    snapshot.weight=options_.weight;
    snapshot.failure_penalty_us=penalty;
    snapshot.score=score;
    snapshot.selectable=
        status_==HealthStatus::Healthy||
        status_==HealthStatus::Suspect||
        (status_==HealthStatus::HalfOpen&&
         !half_open_probe_inflight_);
    return snapshot;
}

void EndpointState::SetWeight(double weight){
    if(!std::isfinite(weight)||weight<=0.0){
        throw std::invalid_argument("endpoint weight must be positive");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    options_.weight=weight;
}

void EndpointState::RefreshHealth(TimePoint now)noexcept{
    if(status_==HealthStatus::Unhealthy&&now>=retry_at_){
        status_=HealthStatus::HalfOpen;
        half_open_probe_inflight_=false;
    }
}

void EndpointState::UpdateEwma(
    std::chrono::microseconds latency
)noexcept{
    double sample=ToMicros(latency);

    if(!has_latency_sample_){
        ewma_latency_us_=sample;
        has_latency_sample_=true;
        return;
    }

    ewma_latency_us_=
        options_.ewma_alpha*sample+
        (1.0-options_.ewma_alpha)*ewma_latency_us_;
}

void EndpointState::DecreaseInflight()noexcept{
    if(inflight_!=0){
        --inflight_;
    }
}

void EndpointState::MarkUnhealthy(TimePoint now)noexcept{
    status_=HealthStatus::Unhealthy;
    retry_at_=now+options_.cooldown;
    half_open_probe_inflight_=false;
}

}
