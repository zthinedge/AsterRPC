#pragma once

#include "calculator.pb.h"
#include "asterrpc/rpc/CallOptions.h"

namespace asterrpc::rpc{
class RpcClient;
}

namespace asterrpc::example::calculator{

class CalculatorStub{
public:
    explicit CalculatorStub(rpc::RpcClient* client);

    AddResponse Add(
        const AddRequest& request,
        rpc::CallOptions options={}
    )const;

private:
    rpc::RpcClient* client_;
};

}
