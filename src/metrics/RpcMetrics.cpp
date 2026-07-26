#include "minirpc/metrics/RpcMetrics.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace minirpc::metrics{
namespace{

constexpr std::array<std::uint64_t,19> kLatencyUpperBoundsUs{
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    25000,
    50000,
    100000,
    250000,
    500000,
    1000000,
    2500000,
    5000000,
    10000000,
    30000000,
    60000000,
    std::numeric_limits<std::uint64_t>::max()
};

void DecrementGauge(std::atomic_int64_t* gauge)noexcept{
    std::int64_t current=gauge->load(std::memory_order_relaxed);

    while(current>0&&!gauge->compare_exchange_weak(
        current,
        current-1,
        std::memory_order_relaxed
    )){}
}

std::uint64_t Percentile(
    const std::array<std::uint64_t,19>& buckets,
    std::uint64_t sample_count,
    std::uint64_t max_latency,
    std::uint64_t numerator
)noexcept{
    if(sample_count==0){
        return 0;
    }

    std::uint64_t rank=
        (sample_count/100)*numerator+
        ((sample_count%100)*numerator+99)/100;
    std::uint64_t cumulative=0;

    for(std::size_t index=0;index<buckets.size();++index){
        cumulative+=buckets[index];

        if(cumulative>=rank){
            if(kLatencyUpperBoundsUs[index]==
               std::numeric_limits<std::uint64_t>::max()){
                return max_latency;
            }

            return kLatencyUpperBoundsUs[index];
        }
    }

    return max_latency;
}

}

struct RpcMetrics::Bucket{
    std::atomic_uint64_t total_requests{0};
    std::atomic_uint64_t successful_requests{0};
    std::atomic_uint64_t failed_requests{0};
    std::atomic_uint64_t timeout_requests{0};
    std::atomic_uint64_t retries{0};
    std::atomic_int64_t inflight_requests{0};
    std::atomic_int64_t active_connections{0};

    std::atomic_uint64_t latency_samples{0};
    std::atomic_uint64_t total_latency_us{0};
    std::atomic_uint64_t max_latency_us{0};
    std::array<std::atomic_uint64_t,kBucketCount> latency_buckets{};
};

double RpcMetricsSnapshot::AverageLatencyMicros()const noexcept{
    if(latency_samples==0){
        return 0.0;
    }

    return static_cast<double>(total_latency_us)/
           static_cast<double>(latency_samples);
}

RpcMetrics::RpcMetrics()
:totals_(std::make_unique<Bucket>()){}

RpcMetrics::~RpcMetrics()=default;

void RpcMetrics::Start(Bucket* bucket)noexcept{
    if(bucket==nullptr){
        return;
    }

    bucket->total_requests.fetch_add(1,std::memory_order_relaxed);
    bucket->inflight_requests.fetch_add(1,std::memory_order_relaxed);
}

void RpcMetrics::Finish(
    Bucket* bucket,
    protocol::StatusCode status,
    std::chrono::microseconds latency
)noexcept{
    if(bucket==nullptr){
        return;
    }

    DecrementGauge(&bucket->inflight_requests);

    if(status==protocol::StatusCode::Ok){
        bucket->successful_requests.fetch_add(
            1,
            std::memory_order_relaxed
        );
    }else{
        bucket->failed_requests.fetch_add(1,std::memory_order_relaxed);

        if(status==protocol::StatusCode::Timeout){
            bucket->timeout_requests.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }
    }

    std::uint64_t latency_us=latency.count()>0
        ?static_cast<std::uint64_t>(latency.count())
        :0;

    bucket->latency_samples.fetch_add(1,std::memory_order_relaxed);
    bucket->total_latency_us.fetch_add(
        latency_us,
        std::memory_order_relaxed
    );

    std::uint64_t current=
        bucket->max_latency_us.load(std::memory_order_relaxed);
    while(current<latency_us&&
          !bucket->max_latency_us.compare_exchange_weak(
              current,
              latency_us,
              std::memory_order_relaxed
          )){}

    auto position=std::lower_bound(
        kLatencyUpperBoundsUs.begin(),
        kLatencyUpperBoundsUs.end(),
        latency_us
    );
    std::size_t index=static_cast<std::size_t>(
        position-kLatencyUpperBoundsUs.begin()
    );
    bucket->latency_buckets[index].fetch_add(
        1,
        std::memory_order_relaxed
    );
}

void RpcMetrics::RequestStarted()noexcept{
    Start(totals_.get());
}

void RpcMetrics::RequestStarted(
    std::string_view service_name,
    std::string_view method_name
)noexcept{
    Start(totals_.get());
    Start(GetOrCreateMethod(service_name,method_name));
}

void RpcMetrics::RequestFinished(
    protocol::StatusCode status,
    std::chrono::microseconds latency
)noexcept{
    Finish(totals_.get(),status,latency);
}

void RpcMetrics::RequestFinished(
    protocol::StatusCode status,
    TimePoint started_at
)noexcept{
    RequestFinished(
        status,
        std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now()-started_at
        )
    );
}

void RpcMetrics::RequestFinished(
    std::string_view service_name,
    std::string_view method_name,
    protocol::StatusCode status,
    std::chrono::microseconds latency
)noexcept{
    Finish(totals_.get(),status,latency);
    Finish(FindMethod(service_name,method_name),status,latency);
}

void RpcMetrics::RequestFinished(
    std::string_view service_name,
    std::string_view method_name,
    protocol::StatusCode status,
    TimePoint started_at
)noexcept{
    RequestFinished(
        service_name,
        method_name,
        status,
        std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now()-started_at
        )
    );
}

void RpcMetrics::RetryStarted()noexcept{
    totals_->retries.fetch_add(1,std::memory_order_relaxed);
}

void RpcMetrics::RetryStarted(
    std::string_view service_name,
    std::string_view method_name
)noexcept{
    totals_->retries.fetch_add(1,std::memory_order_relaxed);
    Bucket* bucket=GetOrCreateMethod(service_name,method_name);
    if(bucket!=nullptr){
        bucket->retries.fetch_add(1,std::memory_order_relaxed);
    }
}

void RpcMetrics::ConnectionOpened()noexcept{
    totals_->active_connections.fetch_add(
        1,
        std::memory_order_relaxed
    );
}

void RpcMetrics::ConnectionClosed()noexcept{
    DecrementGauge(&totals_->active_connections);
}

RpcMetricsSnapshot RpcMetrics::Read(const Bucket* bucket)noexcept{
    RpcMetricsSnapshot snapshot;
    if(bucket==nullptr){
        return snapshot;
    }

    snapshot.total_requests=
        bucket->total_requests.load(std::memory_order_relaxed);
    snapshot.successful_requests=
        bucket->successful_requests.load(std::memory_order_relaxed);
    snapshot.failed_requests=
        bucket->failed_requests.load(std::memory_order_relaxed);
    snapshot.timeout_requests=
        bucket->timeout_requests.load(std::memory_order_relaxed);
    snapshot.retries=bucket->retries.load(std::memory_order_relaxed);
    snapshot.inflight_requests=
        bucket->inflight_requests.load(std::memory_order_relaxed);
    snapshot.active_connections=
        bucket->active_connections.load(std::memory_order_relaxed);
    snapshot.latency_samples=
        bucket->latency_samples.load(std::memory_order_relaxed);
    snapshot.total_latency_us=
        bucket->total_latency_us.load(std::memory_order_relaxed);
    snapshot.max_latency_us=
        bucket->max_latency_us.load(std::memory_order_relaxed);

    std::array<std::uint64_t,kBucketCount> buckets;
    for(std::size_t index=0;index<kBucketCount;++index){
        buckets[index]=bucket->latency_buckets[index].load(
            std::memory_order_relaxed
        );
    }

    snapshot.p50_latency_us=Percentile(
        buckets,
        snapshot.latency_samples,
        snapshot.max_latency_us,
        50
    );
    snapshot.p95_latency_us=Percentile(
        buckets,
        snapshot.latency_samples,
        snapshot.max_latency_us,
        95
    );
    snapshot.p99_latency_us=Percentile(
        buckets,
        snapshot.latency_samples,
        snapshot.max_latency_us,
        99
    );

    return snapshot;
}

RpcMetricsSnapshot RpcMetrics::Snapshot()const noexcept{
    return Read(totals_.get());
}

RpcMetrics::Bucket* RpcMetrics::GetOrCreateMethod(
    std::string_view service_name,
    std::string_view method_name
)noexcept{
    if(service_name.empty()||method_name.empty()){
        return nullptr;
    }

    try{
        std::lock_guard<std::mutex> lock(methods_mutex_);
        auto service=methods_.find(service_name);
        if(service==methods_.end()){
            service=methods_.emplace(
                std::string(service_name),
                MethodMap{}
            ).first;
        }

        auto method=service->second.find(method_name);
        if(method==service->second.end()){
            method=service->second.emplace(
                std::string(method_name),
                std::make_unique<Bucket>()
            ).first;
        }

        return method->second.get();
    }catch(...){
        return nullptr;
    }
}

const RpcMetrics::Bucket* RpcMetrics::FindMethod(
    std::string_view service_name,
    std::string_view method_name
)const noexcept{
    try{
        std::lock_guard<std::mutex> lock(methods_mutex_);
        auto service=methods_.find(service_name);
        if(service==methods_.end()){
            return nullptr;
        }

        auto method=service->second.find(method_name);
        return method==service->second.end()
            ?nullptr
            :method->second.get();
    }catch(...){
        return nullptr;
    }
}

RpcMetrics::Bucket* RpcMetrics::FindMethod(
    std::string_view service_name,
    std::string_view method_name
)noexcept{
    return const_cast<Bucket*>(
        static_cast<const RpcMetrics*>(this)->FindMethod(
            service_name,
            method_name
        )
    );
}

RpcMetricsSnapshot RpcMetrics::MethodSnapshot(
    std::string_view service_name,
    std::string_view method_name
)const noexcept{
    return Read(FindMethod(service_name,method_name));
}

std::vector<RpcMethodMetricsSnapshot>
RpcMetrics::MethodSnapshots()const{
    std::vector<RpcMethodMetricsSnapshot> snapshots;
    std::lock_guard<std::mutex> lock(methods_mutex_);

    for(const auto& service:methods_){
        for(const auto& method:service.second){
            RpcMethodMetricsSnapshot snapshot;
            snapshot.service_name=service.first;
            snapshot.method_name=method.first;
            snapshot.metrics=Read(method.second.get());
            snapshots.push_back(std::move(snapshot));
        }
    }

    return snapshots;
}

}
