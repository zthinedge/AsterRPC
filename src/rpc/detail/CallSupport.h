#pragma once

#include "asterrpc/protocol/RpcMessage.h"
#include "asterrpc/rpc/CallOptions.h"

#include <cstdint>
#include <string>

namespace asterrpc::rpc::detail{

std::uint64_t CurrentTimeMicros()noexcept;
std::uint64_t ResolveDeadline(const CallOptions& options);
bool DeadlineReached(std::uint64_t deadline_us)noexcept;

protocol::RpcMessage MakeResponse(
    const protocol::RpcMessage& request
);

protocol::RpcMessage MakeErrorResponse(
    protocol::StatusCode status,
    std::string error_text,
    std::uint64_t request_id=0
);

protocol::RpcMessage MakeErrorResponse(
    const protocol::RpcMessage& request,
    protocol::StatusCode status,
    std::string error_text
);

}
