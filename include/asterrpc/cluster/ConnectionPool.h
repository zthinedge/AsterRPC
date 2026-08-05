#pragma once

#include "asterrpc/cluster/Endpoint.h"
#include "asterrpc/cluster/RetryPolicy.h"
#include "asterrpc/metrics/RpcMetrics.h"
#include "asterrpc/protocol/RpcMessage.h"
#include "asterrpc/rpc/CallOptions.h"

#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace asterrpc::net{
class EventLoop;
}

namespace asterrpc::cluster{

struct ConnectionPoolOptions{
    std::size_t max_connections=4;
    std::chrono::milliseconds idle_timeout{60000};
    std::chrono::milliseconds reap_interval{10000};

    bool operator==(const ConnectionPoolOptions& other)const noexcept;
    bool operator!=(const ConnectionPoolOptions& other)const noexcept;
};

struct ConnectionPoolStats{
    std::size_t connections=0;
    std::size_t connected=0;
    std::size_t in_flight=0;
    std::size_t retries=0;
};

class ConnectionPool{
public:
    using ResponseFuture=std::future<protocol::RpcMessage>;
    using ResponseCallback=
        std::function<void(protocol::RpcMessage)>;

    ConnectionPool(
        net::EventLoop* loop,
        Endpoint endpoint,
        ConnectionPoolOptions options={}
    );
    ~ConnectionPool();

    ConnectionPool(const ConnectionPool&)=delete;
    ConnectionPool& operator=(const ConnectionPool&)=delete;

    protocol::RpcMessage Call(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions call_options={},
        RetryPolicy retry_policy={}
    );

    ResponseFuture FutureCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions call_options={},
        RetryPolicy retry_policy={}
    );

    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        ResponseCallback callback,
        rpc::CallOptions call_options={},
        RetryPolicy retry_policy={}
    );

    const Endpoint& GetEndpoint()const noexcept;
    const ConnectionPoolOptions& Options()const noexcept;
    ConnectionPoolStats GetStats()const noexcept;
    metrics::RpcMetricsSnapshot GetMetrics()const noexcept;
    metrics::RpcMetricsSnapshot GetMethodMetrics(
        std::string_view service_name,
        std::string_view method_name
    )const noexcept;
    std::vector<metrics::RpcMethodMetricsSnapshot>
    GetAllMethodMetrics()const;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}
