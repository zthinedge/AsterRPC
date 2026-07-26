#pragma once

#include "minirpc/metrics/RpcMetrics.h"
#include "minirpc/net/TcpServer.h"
#include "minirpc/protocol/RpcCodec.h"
#include "minirpc/rpc/ServiceDispatcher.h"

#include <string>
#include <string_view>
#include <vector>

namespace minirpc::net{
class EventLoop;
class InetAddress;
}

namespace minirpc::rpc{

class RpcServer{
public:
    using MethodHandler=ServiceDispatcher::MethodHandler;

    RpcServer(
        net::EventLoop* loop,
        const net::InetAddress& addr
    );

    void RegisterMethod(
        std::string service_name,
        std::string method_name,
        MethodHandler handler
    );

    void Start();
    metrics::RpcMetricsSnapshot GetMetrics()const noexcept;
    metrics::RpcMetricsSnapshot GetMethodMetrics(
        std::string_view service_name,
        std::string_view method_name
    )const noexcept;
    std::vector<metrics::RpcMethodMetricsSnapshot>
    GetAllMethodMetrics()const;

private:
    void HandleMessage(
        net::TcpConnection* connection,
        net::Buffer* buffer
    );

    net::TcpServer tcp_server_;
    protocol::RpcCodec codec_;
    ServiceDispatcher dispatcher_;
    metrics::RpcMetrics metrics_;
};

}
