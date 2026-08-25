#pragma once

#include "asterrpc/net/TcpClient.h"
#include "asterrpc/protocol/RpcCodec.h"
#include "asterrpc/rpc/CallOptions.h"
#include "asterrpc/rpc/PendingCalls.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

namespace asterrpc::net{
class EventLoop;
class InetAddress;
}

namespace asterrpc::rpc{

class RpcClient{
public:
    using ResponseFuture=PendingCalls::ResponseFuture;
    using ResponseCallback=PendingCalls::ResponseCallback;
    using ConnectionCallback=std::function<void()>;
    using CloseCallback=std::function<void()>;
    using ErrorCallback=std::function<void(int)>;

    RpcClient(
        net::EventLoop* loop,
        const net::InetAddress& server_addr
    );

    void Connect();
    void Disconnect();

    protocol::RpcMessage Call(
        std::string service_name,
        std::string method_name,
        std::string payload,
        CallOptions options={}
    );

    ResponseFuture FutureCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        CallOptions options={}
    );

    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        ResponseCallback callback,
        CallOptions options={}
    );

    bool IsConnected()const noexcept;

    void SetConnectionCallback(ConnectionCallback callback);
    void SetCloseCallback(CloseCallback callback);
    void SetErrorCallback(ErrorCallback callback);

private:
    std::uint64_t NextRequestId()noexcept;

    void StartCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        CallOptions options,
        ResponseCallback completion
    );

    protocol::RpcMessage MakeRequest(
        std::string service_name,
        std::string method_name,
        std::string payload,
        std::uint64_t deadline_us
    );

    void AddTimeout(
        std::uint64_t request_id,
        std::uint64_t deadline_us
    );

    void SendRequest(
        std::uint64_t request_id,
        std::string bytes
    );

    void HandleConnectionLost();

    void HandleMessage(
        net::TcpConnection* connection,
        net::Buffer* buffer
    );

    net::EventLoop* loop_;
    net::TcpClient tcp_client_;
    protocol::RpcCodec codec_;
    std::atomic_uint64_t next_request_id_;
    std::atomic_bool connected_;
    PendingCalls pending_calls_;

    ConnectionCallback connection_callback_;
    CloseCallback close_callback_;
    ErrorCallback error_callback_;
};

}
