#include "asterrpc/rpc/PendingCalls.h"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{
namespace{

protocol::RpcMessage MakeErrorResponse(
    std::uint64_t request_id,
    protocol::StatusCode status_code,
    const std::string& error_text
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.request_id=request_id;
    response.meta.status_code=status_code;
    response.meta.error_text=error_text;
    return response;
}

std::uint64_t CurrentTimeMicros(){
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

}

PendingCalls::ResponseFuture PendingCalls::Add(
    std::uint64_t request_id
){
    return Add(request_id,0);
}

PendingCalls::ResponseFuture PendingCalls::Add(
    std::uint64_t request_id,
    std::uint64_t deadline_us
){
    PendingCall call;
    call.deadline_us=deadline_us;
    ResponseFuture future=call.promise.get_future();
    Insert(request_id,std::move(call));

    return future;
}

void PendingCalls::Add(
    std::uint64_t request_id,
    ResponseCallback callback
){
    Add(request_id,0,std::move(callback));
}

void PendingCalls::Add(
    std::uint64_t request_id,
    std::uint64_t deadline_us,
    ResponseCallback callback
){
    if(!callback){
        throw std::invalid_argument("rpc response callback is empty");
    }

    PendingCall call;
    call.deadline_us=deadline_us;
    call.callback=std::move(callback);
    Insert(request_id,std::move(call));
}

bool PendingCalls::SetTimeoutCancel(
    std::uint64_t request_id,
    std::function<void()> cancel
){
    if(!cancel){
        throw std::invalid_argument("rpc timeout cancel is empty");
    }

    std::lock_guard<std::mutex>lock(mutex_);
    auto pending=calls_.find(request_id);

    if(pending==calls_.end()){
        return false;
    }

    pending->second.cancel_timeout=std::move(cancel);
    return true;
}

bool PendingCalls::Complete(protocol::RpcMessage response){
    PendingCall call;
    bool expired=false;

    {
        std::lock_guard<std::mutex>lock(mutex_);
        auto pending=calls_.find(response.request_id);

        if(pending==calls_.end()){
            return false;
        }

        expired=pending->second.deadline_us!=0&&
                CurrentTimeMicros()>=pending->second.deadline_us;
        call=std::move(pending->second);
        calls_.erase(pending);
    }

    if(expired){
        response=MakeErrorResponse(
            response.request_id,
            protocol::RpcError::Timeout,
            "rpc deadline exceeded"
        );
    }

    Finish(std::move(call),std::move(response));
    return true;
}

bool PendingCalls::Expire(std::uint64_t request_id){
    PendingCall call;

    {
        std::lock_guard<std::mutex>lock(mutex_);
        auto pending=calls_.find(request_id);

        if(pending==calls_.end()||pending->second.deadline_us==0){
            return false;
        }

        call=std::move(pending->second);
        calls_.erase(pending);
    }

    Finish(
        std::move(call),
        MakeErrorResponse(
            request_id,
            protocol::RpcError::Timeout,
            "rpc deadline exceeded"
        )
    );
    return true;
}

bool PendingCalls::Fail(
    std::uint64_t request_id,
    protocol::StatusCode status_code,
    const std::string& error_text
){
    return Complete(
        MakeErrorResponse(request_id,status_code,error_text)
    );
}

void PendingCalls::FailAll(
    protocol::StatusCode status_code,
    const std::string& error_text
){
    CallMap calls;

    {
        std::lock_guard<std::mutex>lock(mutex_);
        calls.swap(calls_);
    }

    for(auto& item:calls){
        Finish(
            std::move(item.second),
            MakeErrorResponse(item.first,status_code,error_text)
        );
    }
}

std::size_t PendingCalls::Size()const{
    std::lock_guard<std::mutex>lock(mutex_);
    return calls_.size();
}

void PendingCalls::Insert(
    std::uint64_t request_id,
    PendingCall call
){
    if(request_id==0){
        throw std::invalid_argument("rpc request id must not be zero");
    }

    std::lock_guard<std::mutex>lock(mutex_);
    auto result=calls_.emplace(request_id,std::move(call));

    if(!result.second){
        throw std::logic_error("rpc request id already pending");
    }
}

void PendingCalls::Finish(
    PendingCall call,
    protocol::RpcMessage response
)noexcept{
    if(call.cancel_timeout){
        try{
            call.cancel_timeout();
        }catch(...){
        }
    }

    try{
        if(call.callback){
            call.callback(std::move(response));
            return;
        }

        call.promise.set_value(std::move(response));
    }catch(...){
        // 用户回调的异常不能中断 EventLoop 或其他 PendingCall。
    }
}

}
