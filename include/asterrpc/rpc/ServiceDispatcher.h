#pragma once

#include "asterrpc/protocol/RpcMessage.h"

#include <functional>
#include <string>
#include <unordered_map>

namespace asterrpc::rpc{

class ServiceDispatcher{
public:
    using MethodHandler=
        std::function<std::string(const std::string&)>;

    void RegisterMethod(
        std::string service_name,
        std::string method_name,
        MethodHandler handler
    );

    protocol::RpcMessage Dispatch(
        const protocol::RpcMessage& request
    )const;

private:
    using MethodMap=
        std::unordered_map<std::string,MethodHandler>;

    std::unordered_map<std::string,MethodMap> services_;
};

}
