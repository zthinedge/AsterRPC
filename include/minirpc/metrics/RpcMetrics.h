#pragma once

#include "minirpc/protocol/RpcMeta.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace minirpc::metrics{

struct RpcMetricsSnapshot{
    std::uint64_t total_requests=0;
    std::uint64_t successful_requests=0;
    // 所有非 Ok 请求，包含 timeout_requests。
    std::uint64_t failed_requests=0;
    std::uint64_t timeout_requests=0;
    std::uint64_t retries=0;
    std::int64_t inflight_requests=0;
    std::int64_t active_connections=0;

    // 延迟单位为微秒，分位数是固定直方图桶的近似上界。
    std::uint64_t latency_samples=0;
    std::uint64_t total_latency_us=0;
    std::uint64_t max_latency_us=0;
    std::uint64_t p50_latency_us=0;
    std::uint64_t p95_latency_us=0;
    std::uint64_t p99_latency_us=0;

    double AverageLatencyMicros()const noexcept;
};

struct RpcMethodMetricsSnapshot{
    std::string service_name;
    std::string method_name;
    RpcMetricsSnapshot metrics;
};

class RpcMetrics{
public:
    using Clock=std::chrono::steady_clock;
    using TimePoint=Clock::time_point;

    RpcMetrics();
    ~RpcMetrics();

    RpcMetrics(const RpcMetrics&)=delete;
    RpcMetrics& operator=(const RpcMetrics&)=delete;
    RpcMetrics(RpcMetrics&&)=delete;
    RpcMetrics& operator=(RpcMetrics&&)=delete;

    void RequestStarted()noexcept;
    void RequestStarted(
        std::string_view service_name,
        std::string_view method_name
    )noexcept;

    void RequestFinished(
        protocol::StatusCode status,
        std::chrono::microseconds latency
    )noexcept;

    void RequestFinished(
        protocol::StatusCode status,
        TimePoint started_at
    )noexcept;

    void RequestFinished(
        std::string_view service_name,
        std::string_view method_name,
        protocol::StatusCode status,
        std::chrono::microseconds latency
    )noexcept;

    void RequestFinished(
        std::string_view service_name,
        std::string_view method_name,
        protocol::StatusCode status,
        TimePoint started_at
    )noexcept;

    void RetryStarted()noexcept;
    void RetryStarted(
        std::string_view service_name,
        std::string_view method_name
    )noexcept;

    void ConnectionOpened()noexcept;
    void ConnectionClosed()noexcept;

    RpcMetricsSnapshot Snapshot()const noexcept;
    RpcMetricsSnapshot MethodSnapshot(
        std::string_view service_name,
        std::string_view method_name
    )const noexcept;

    std::vector<RpcMethodMetricsSnapshot> MethodSnapshots()const;

private:
    static constexpr std::size_t kBucketCount=19;

    struct Bucket;
    using MethodMap=
        std::map<
            std::string,
            std::unique_ptr<Bucket>,
            std::less<>
        >;
    using ServiceMap=
        std::map<std::string,MethodMap,std::less<>>;

    Bucket* GetOrCreateMethod(
        std::string_view service_name,
        std::string_view method_name
    )noexcept;

    const Bucket* FindMethod(
        std::string_view service_name,
        std::string_view method_name
    )const noexcept;
    Bucket* FindMethod(
        std::string_view service_name,
        std::string_view method_name
    )noexcept;

    static void Start(Bucket* bucket)noexcept;
    static void Finish(
        Bucket* bucket,
        protocol::StatusCode status,
        std::chrono::microseconds latency
    )noexcept;
    static RpcMetricsSnapshot Read(const Bucket* bucket)noexcept;

    std::unique_ptr<Bucket> totals_;
    mutable std::mutex methods_mutex_;
    ServiceMap methods_;
};

}
