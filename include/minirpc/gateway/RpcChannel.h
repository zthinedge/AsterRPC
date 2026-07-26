#pragma once

#include "minirpc/protocol/RpcMessage.h"
#include "minirpc/rpc/CallOptions.h"

#include <functional>
#include <string>

namespace minirpc::gateway{

class RpcChannel{
public:
    using Completion=std::function<void(protocol::RpcMessage)>;

    virtual ~RpcChannel()=default;

    virtual void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions options,
        Completion completion
    )=0;
};

}
