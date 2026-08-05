#include "rpc/detail/CallSupport.h"

#include <chrono>
#include <limits>
#include <stdexcept>
#include <utility>

namespace asterrpc::rpc::detail{

std::uint64_t CurrentTimeMicros()noexcept{
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

std::uint64_t ResolveDeadline(const CallOptions& options){
    if(options.timeout<std::chrono::microseconds::zero()){
        throw std::invalid_argument("rpc timeout must not be negative");
    }
    if(options.timeout==std::chrono::microseconds::zero()){
        return options.deadline_us;
    }

    const std::uint64_t now=CurrentTimeMicros();
    const std::uint64_t timeout=
        static_cast<std::uint64_t>(options.timeout.count());
    const std::uint64_t relative_deadline=
        timeout>std::numeric_limits<std::uint64_t>::max()-now
        ?std::numeric_limits<std::uint64_t>::max()
        :now+timeout;

    return options.deadline_us==0||relative_deadline<options.deadline_us
        ?relative_deadline
        :options.deadline_us;
}

bool DeadlineReached(std::uint64_t deadline_us)noexcept{
    return deadline_us!=0&&CurrentTimeMicros()>=deadline_us;
}

protocol::RpcMessage MakeResponse(
    const protocol::RpcMessage& request
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.codec=request.codec;
    response.request_id=request.request_id;
    return response;
}

protocol::RpcMessage MakeErrorResponse(
    protocol::StatusCode status,
    std::string error_text,
    std::uint64_t request_id
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.request_id=request_id;
    response.meta.status_code=status;
    response.meta.error_text=std::move(error_text);
    return response;
}

protocol::RpcMessage MakeErrorResponse(
    const protocol::RpcMessage& request,
    protocol::StatusCode status,
    std::string error_text
){
    protocol::RpcMessage response=MakeResponse(request);
    response.meta.status_code=status;
    response.meta.error_text=std::move(error_text);
    return response;
}

}
