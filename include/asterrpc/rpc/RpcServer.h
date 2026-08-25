#pragma once

#include "asterrpc/net/TcpServer.h"
#include "asterrpc/protocol/RpcCodec.h"
#include "asterrpc/rpc/ServiceDispatcher.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace asterrpc::common{
class ThreadPool;
}

namespace asterrpc::net{
class EventLoop;
class InetAddress;
class TcpConnection;
}

namespace asterrpc::rpc{

struct RpcServerOptions{
    net::TcpServerOptions tcp;
    std::size_t business_threads=0;
    std::size_t business_queue_capacity=65536;
};

class RpcServer{
public:
    using MethodHandler=ServiceDispatcher::MethodHandler;

    RpcServer(
        net::EventLoop* loop,
        const net::InetAddress& addr,
        RpcServerOptions options={}
    );
    ~RpcServer();

    void RegisterMethod(
        std::string service_name,
        std::string method_name,
        MethodHandler handler
    );

    void Start();
    std::size_t IoThreadCount()const noexcept;
    std::vector<std::size_t> IoConnectionCounts()const;
    std::size_t BusinessThreadCount()const noexcept;
    std::size_t PendingBusinessTasks()const;

private:
    void HandleMessage(
        net::TcpConnection* connection,
        net::Buffer* buffer
    );

    void ProcessRequest(
        std::weak_ptr<net::TcpConnection> connection,
        protocol::RpcMessage request
    );

    void SendResponse(
        std::weak_ptr<net::TcpConnection> connection,
        const protocol::RpcMessage& request,
        protocol::RpcMessage response
    );

    net::TcpServer tcp_server_;
    protocol::RpcCodec codec_;
    ServiceDispatcher dispatcher_;
    std::unique_ptr<common::ThreadPool> business_pool_;
};

}
