#include "asterrpc/rpc/RpcClient.h"

#include "asterrpc/net/Buffer.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/TcpConnection.h"
#include "rpc/detail/CallSupport.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{

RpcClient::RpcClient(
    net::EventLoop* loop,
    const net::InetAddress& server_addr
):loop_(loop),
  tcp_client_(loop,server_addr),
  next_request_id_(1),
  connected_(false){
    tcp_client_.SetConnectionCallback(
        [this](net::TcpConnection*){
            connected_.store(true);

            if(connection_callback_){
                connection_callback_();
            }
        }
    );

    tcp_client_.SetMessageCallback(
        [this](net::TcpConnection* connection,net::Buffer* buffer){
            HandleMessage(connection,buffer);
        }
    );

    tcp_client_.SetCloseCallback([this](){
        HandleConnectionLost();
        if(close_callback_){
            close_callback_();
        }
    });

    tcp_client_.SetErrorCallback([this](int error){
        HandleConnectionLost();
        if(error_callback_){
            error_callback_(error);
        }
    });
}

void RpcClient::Connect(){
    loop_->RunInLoop([this](){
        tcp_client_.Connect();
    });
}

void RpcClient::Disconnect(){
    loop_->RunInLoop([this](){
        tcp_client_.Disconnect();
    });
}

protocol::RpcMessage RpcClient::Call(
    std::string service_name,
    std::string method_name,
    std::string payload,
    CallOptions options
){
    if(loop_->IsInLoopThread()){
        throw std::logic_error(
            "synchronous rpc call in EventLoop thread"
        );
    }

    return FutureCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        options
    ).get();
}

RpcClient::ResponseFuture RpcClient::FutureCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    CallOptions options
){
    auto promise=
        std::make_shared<std::promise<protocol::RpcMessage>>();
    ResponseFuture future=promise->get_future();

    StartCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        options,
        [promise](protocol::RpcMessage response){
            promise->set_value(std::move(response));
        }
    );

    return future;
}

void RpcClient::AsyncCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    ResponseCallback callback,
    CallOptions options
){
    if(!callback){
        throw std::invalid_argument("rpc response callback is empty");
    }

    StartCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        options,
        std::move(callback)
    );
}

void RpcClient::StartCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    CallOptions options,
    ResponseCallback completion
){
    const std::uint64_t deadline_us=detail::ResolveDeadline(options);
    protocol::RpcMessage request=MakeRequest(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        deadline_us
    );
    std::string bytes=codec_.Encode(request);

    pending_calls_.Add(
        request.request_id,
        deadline_us,
        std::move(completion)
    );

    if(deadline_us!=0){
        AddTimeout(request.request_id,deadline_us);
    }

    if(detail::DeadlineReached(deadline_us)){
        return;
    }

    SendRequest(request.request_id,std::move(bytes));
}

protocol::RpcMessage RpcClient::MakeRequest(
    std::string service_name,
    std::string method_name,
    std::string payload,
    std::uint64_t deadline_us
){
    protocol::RpcMessage request;
    request.message_type=protocol::MessageType::Request;
    request.request_id=NextRequestId();
    request.meta.service_name=std::move(service_name);
    request.meta.method_name=std::move(method_name);
    request.meta.deadline_us=deadline_us;
    request.payload=std::move(payload);
    return request;
}

void RpcClient::AddTimeout(
    std::uint64_t request_id,
    std::uint64_t deadline_us
){
    std::uint64_t now=detail::CurrentTimeMicros();
    std::uint64_t remaining=deadline_us>now?deadline_us-now:0;

    net::EventLoop::TimerId timer_id=loop_->RunAfter(
        std::chrono::microseconds(remaining),
        [this,request_id](){
            pending_calls_.Expire(request_id);
        }
    );

    bool attached=pending_calls_.SetTimeoutCancel(
        request_id,
        [this,timer_id](){
            loop_->CancelTimer(timer_id);
        }
    );

    if(!attached){
        loop_->CancelTimer(timer_id);
    }
}

void RpcClient::SendRequest(
    std::uint64_t request_id,
    std::string bytes
){
    loop_->RunInLoop(
        [this,request_id,bytes=std::move(bytes)](){
            if(!tcp_client_.IsConnected()){
                pending_calls_.Fail(
                    request_id,
                    protocol::StatusCode::ConnectionFailed,
                    "rpc client is not connected"
                );
                return;
            }

            tcp_client_.Send(bytes);
        }
    );
}

void RpcClient::HandleConnectionLost(){
    connected_.store(false);
    pending_calls_.FailAll(
        protocol::StatusCode::ConnectionFailed,
        "rpc connection lost"
    );
}

bool RpcClient::IsConnected()const noexcept{
    return connected_.load();
}

void RpcClient::SetConnectionCallback(ConnectionCallback callback){
    connection_callback_=std::move(callback);
}

void RpcClient::SetCloseCallback(CloseCallback callback){
    close_callback_=std::move(callback);
}

void RpcClient::SetErrorCallback(ErrorCallback callback){
    error_callback_=std::move(callback);
}

std::uint64_t RpcClient::NextRequestId()noexcept{
    std::uint64_t request_id=next_request_id_.fetch_add(1);

    while(request_id==0){
        request_id=next_request_id_.fetch_add(1);
    }

    return request_id;
}

void RpcClient::HandleMessage(
    net::TcpConnection* connection,
    net::Buffer* buffer
){
    while(true){
        protocol::RpcMessage response;
        std::string error;

        protocol::DecodeStatus status=codec_.DecodeOne(
            buffer,
            &response,
            &error
        );

        if(status==protocol::DecodeStatus::NeedMoreData){
            return;
        }

        if(status==protocol::DecodeStatus::ProtocolError||
           response.message_type!=protocol::MessageType::Response){
            connection->Close();
            return;
        }

        pending_calls_.Complete(std::move(response));
    }
}

}
