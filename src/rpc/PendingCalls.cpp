#include "asterrpc/rpc/PendingCalls.h"
#include "rpc/detail/CallSupport.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{

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

        expired=detail::DeadlineReached(
            pending->second.deadline_us
        );
        call=std::move(pending->second);
        calls_.erase(pending);
    }

    if(expired){
        response=detail::MakeErrorResponse(
            protocol::RpcError::Timeout,
            "rpc deadline exceeded",
            response.request_id
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
        detail::MakeErrorResponse(
            protocol::RpcError::Timeout,
            "rpc deadline exceeded",
            request_id
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
        detail::MakeErrorResponse(status_code,error_text,request_id)
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
            detail::MakeErrorResponse(
                status_code,
                error_text,
                item.first
            )
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
