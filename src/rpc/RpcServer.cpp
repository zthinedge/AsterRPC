#include "asterrpc/rpc/RpcServer.h"

#include "asterrpc/common/ThreadPool.h"
#include "asterrpc/net/Buffer.h"
#include "asterrpc/net/TcpConnection.h"
#include "rpc/detail/CallSupport.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{
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

        std::weak_ptr<net::TcpConnection> weak_connection=
            connection->WeakFromThis();

        if(detail::DeadlineReached(request.meta.deadline_us)){
            protocol::RpcMessage response=detail::MakeErrorResponse(
                request,
                protocol::RpcError::Timeout,
                "rpc request deadline exceeded"
            );
            SendResponse(
                weak_connection,
                request,
                std::move(response)
            );
            continue;
        }

        if(business_pool_==nullptr){
            ProcessRequest(
                std::move(weak_connection),
                std::move(request)
            );
            continue;
        }

        auto queued_request=
            std::make_shared<protocol::RpcMessage>(std::move(request));

        bool accepted=business_pool_->TrySubmit(
            [
                this,
                weak_connection,
                queued_request
            ]()mutable{
                ProcessRequest(
                    std::move(weak_connection),
                    std::move(*queued_request)
                );
            }
        );

        if(!accepted){
            protocol::RpcMessage response=detail::MakeErrorResponse(
                *queued_request,
                protocol::RpcError::InternalError,
                "rpc business queue is full"
            );
            SendResponse(
                std::move(weak_connection),
                *queued_request,
                std::move(response)
            );
        }
    }
}

void RpcServer::ProcessRequest(
    std::weak_ptr<net::TcpConnection> connection,
    protocol::RpcMessage request
){
    protocol::RpcMessage response=
        detail::DeadlineReached(request.meta.deadline_us)
        ?detail::MakeErrorResponse(
            request,
            protocol::RpcError::Timeout,
            "rpc request deadline exceeded"
        )
        :dispatcher_.Dispatch(request);

    SendResponse(
        std::move(connection),
        request,
        std::move(response)
    );
}

void RpcServer::SendResponse(
    std::weak_ptr<net::TcpConnection> connection,
    const protocol::RpcMessage& request,
    protocol::RpcMessage response
){
    std::string bytes;
    try{
        bytes=codec_.Encode(response);
    }catch(const std::exception& error){
        response=detail::MakeErrorResponse(
            request,
            protocol::RpcError::InternalError,
            error.what()
        );
        bytes=codec_.Encode(response);
    }

    if(auto current=connection.lock()){
        current->Send(bytes);
    }
}

}
