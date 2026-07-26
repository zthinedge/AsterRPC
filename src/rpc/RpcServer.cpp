#include "minirpc/rpc/RpcServer.h"

#include "minirpc/health/HealthService.h"
#include "minirpc/net/Buffer.h"
#include "minirpc/net/TcpConnection.h"

#include <chrono>
#include <utility>

namespace minirpc::rpc{
namespace{

std::uint64_t CurrentTimeMicros(){
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

bool IsExpired(const protocol::RpcMessage& request){
    return request.meta.deadline_us!=0&&
           CurrentTimeMicros()>=request.meta.deadline_us;
}

protocol::RpcMessage MakeTimeoutResponse(
    const protocol::RpcMessage& request
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.codec=request.codec;
    response.request_id=request.request_id;
    response.meta.status_code=protocol::RpcError::Timeout;
    response.meta.error_text="rpc request deadline exceeded";
    return response;
}

}

RpcServer::RpcServer(
    net::EventLoop* loop,
    const net::InetAddress& addr
):tcp_server_(loop,addr){
    tcp_server_.SetMessageCallback(
        [this](net::TcpConnection* connection,net::Buffer* buffer){
            HandleMessage(connection,buffer);
        }
    );
    tcp_server_.SetConnectionCallback([this](){
        metrics_.ConnectionOpened();
    });
    tcp_server_.SetCloseCallback([this](){
        metrics_.ConnectionClosed();
    });
    health::HealthService::RegisterTo(this);
}

void RpcServer::RegisterMethod(
    std::string service_name,
    std::string method_name,
    MethodHandler handler
){
    dispatcher_.RegisterMethod(
        std::move(service_name),
        std::move(method_name),
        std::move(handler)
    );
}

void RpcServer::Start(){
    tcp_server_.Start();
}

metrics::RpcMetricsSnapshot RpcServer::GetMetrics()const noexcept{
    return metrics_.Snapshot();
}

void RpcServer::HandleMessage(
    net::TcpConnection* connection,
    net::Buffer* buffer
){
    while(true){
        protocol::RpcMessage request;
        std::string error;

        protocol::DecodeStatus status=codec_.DecodeOne(
            buffer,
            &request,
            &error
        );

        if(status==protocol::DecodeStatus::NeedMoreData){
            return;
        }

        if(status==protocol::DecodeStatus::ProtocolError||
           request.message_type!=protocol::MessageType::Request){
            connection->Close();
            return;
        }

        auto started_at=metrics::RpcMetrics::Clock::now();
        metrics_.RequestStarted();

        if(IsExpired(request)){
            protocol::RpcMessage response=MakeTimeoutResponse(request);
            metrics_.RequestFinished(
                response.meta.status_code,
                started_at
            );
            connection->Send(codec_.Encode(response));
            continue;
        }

        protocol::RpcMessage response=dispatcher_.Dispatch(request);
        metrics_.RequestFinished(
            response.meta.status_code,
            started_at
        );
        connection->Send(codec_.Encode(response));
    }
}

}
