#include "asterrpc/rpc/RpcServer.h"

#include "asterrpc/common/ThreadPool.h"
#include "asterrpc/health/HealthService.h"
#include "asterrpc/net/Buffer.h"
#include "asterrpc/net/TcpConnection.h"
#include "asterrpc/trace/TraceContext.h"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{
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

protocol::RpcMessage MakeInternalErrorResponse(
    const protocol::RpcMessage& request,
    const std::string& error
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.codec=request.codec;
    response.request_id=request.request_id;
    response.meta.status_code=protocol::RpcError::InternalError;
    response.meta.error_text=error;
    return response;
}

void SetResponseTrace(
    protocol::RpcMessage* response,
    const trace::TraceContext& context
){
    response->meta.trace_id=context.trace_id;
    response->meta.span_id=context.span_id;
    response->meta.parent_span_id=context.parent_span_id;
    response->meta.deadline_us=context.deadline_us;
}

}

RpcServer::RpcServer(
    net::EventLoop* loop,
    const net::InetAddress& addr,
    RpcServerOptions options
):tcp_server_(loop,addr,options.tcp){
    if(options.business_threads!=0&&
       options.business_queue_capacity==0){
        throw std::invalid_argument(
            "rpc business queue capacity must be positive"
        );
    }

    if(options.business_threads!=0){
        business_pool_=std::make_unique<common::ThreadPool>(
            options.business_threads,
            options.business_queue_capacity
        );
    }

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

RpcServer::~RpcServer()=default;

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

metrics::RpcMetricsSnapshot RpcServer::GetMethodMetrics(
    std::string_view service_name,
    std::string_view method_name
)const noexcept{
    return metrics_.MethodSnapshot(service_name,method_name);
}

std::vector<metrics::RpcMethodMetricsSnapshot>
RpcServer::GetAllMethodMetrics()const{
    return metrics_.MethodSnapshots();
}

std::size_t RpcServer::IoThreadCount()const noexcept{
    return tcp_server_.IoThreadCount();
}

std::vector<std::size_t>
RpcServer::IoConnectionCounts()const{
    return tcp_server_.IoConnectionCounts();
}

std::size_t RpcServer::BusinessThreadCount()const noexcept{
    return business_pool_==nullptr?0:business_pool_->ThreadCount();
}

std::size_t RpcServer::PendingBusinessTasks()const{
    return business_pool_==nullptr?0:business_pool_->PendingTasks();
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
        metrics_.RequestStarted(
            request.meta.service_name,
            request.meta.method_name
        );

        trace::TraceContext server_span=trace::CreateServerSpan(
            request.meta.trace_id,
            request.meta.span_id,
            request.meta.deadline_us
        );
        std::weak_ptr<net::TcpConnection> weak_connection=
            connection->WeakFromThis();

        if(IsExpired(request)){
            protocol::RpcMessage response=MakeTimeoutResponse(request);
            SendResponse(
                weak_connection,
                request,
                std::move(response),
                started_at,
                server_span
            );
            continue;
        }

        if(business_pool_==nullptr){
            ProcessRequest(
                std::move(weak_connection),
                std::move(request),
                started_at,
                std::move(server_span)
            );
            continue;
        }

        auto queued_request=
            std::make_shared<protocol::RpcMessage>(std::move(request));

        bool accepted=business_pool_->TrySubmit(
            [
                this,
                weak_connection,
                queued_request,
                started_at,
                server_span
            ]()mutable{
                ProcessRequest(
                    std::move(weak_connection),
                    std::move(*queued_request),
                    started_at,
                    std::move(server_span)
                );
            }
        );

        if(!accepted){
            protocol::RpcMessage response=MakeInternalErrorResponse(
                *queued_request,
                "rpc business queue is full"
            );
            SendResponse(
                std::move(weak_connection),
                *queued_request,
                std::move(response),
                started_at,
                server_span
            );
        }
    }
}

void RpcServer::ProcessRequest(
    std::weak_ptr<net::TcpConnection> connection,
    protocol::RpcMessage request,
    metrics::RpcMetrics::TimePoint started_at,
    trace::TraceContext server_span
){
    trace::TraceScope trace_scope(server_span);

    protocol::RpcMessage response=IsExpired(request)
        ?MakeTimeoutResponse(request)
        :dispatcher_.Dispatch(request);

    SendResponse(
        std::move(connection),
        request,
        std::move(response),
        started_at,
        server_span
    );
}

void RpcServer::SendResponse(
    std::weak_ptr<net::TcpConnection> connection,
    const protocol::RpcMessage& request,
    protocol::RpcMessage response,
    metrics::RpcMetrics::TimePoint started_at,
    const trace::TraceContext& server_span
){
    SetResponseTrace(&response,server_span);

    std::string bytes;
    try{
        bytes=codec_.Encode(response);
    }catch(const std::exception& error){
        response=MakeInternalErrorResponse(request,error.what());
        SetResponseTrace(&response,server_span);
        bytes=codec_.Encode(response);
    }

    metrics_.RequestFinished(
        request.meta.service_name,
        request.meta.method_name,
        response.meta.status_code,
        started_at
    );

    if(auto current=connection.lock()){
        current->Send(bytes);
    }
}

}
