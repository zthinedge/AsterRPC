#pragma once

#include <cstdint>
#include <string>

namespace asterrpc::protocol{

enum class RpcError : std::int32_t{
    Ok=0,
    DecodeError=1,
    ServiceNotFound=2,
    MethodNotFound=3,
    InvokeError=4,
    InternalError=5,
    Timeout=6,
    ConnectionFailed=7
};

using StatusCode=RpcError;

struct RpcMeta{
    std::string service_name;
    std::string method_name;

    StatusCode status_code=StatusCode::Ok;
    std::string error_text;
    std::uint64_t deadline_us=0;

    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
};

}
