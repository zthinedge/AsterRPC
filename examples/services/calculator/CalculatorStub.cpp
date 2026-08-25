#include "CalculatorStub.h"

#include "CalculatorService.h"
#include "asterrpc/rpc/RpcClient.h"

#include <stdexcept>
#include <string>
#include <utility>

namespace asterrpc::example::calculator{

CalculatorStub::CalculatorStub(rpc::RpcClient* client)
    :client_(client){
    if(client_==nullptr){
        throw std::invalid_argument("rpc client is null");
    }
}

AddResponse CalculatorStub::Add(
    const AddRequest& request,
    rpc::CallOptions options
)const{
    std::string payload;
    if(!request.SerializeToString(&payload)){
        throw std::runtime_error(
            "failed to serialize calculator Add request"
        );
    }

    protocol::RpcMessage response=client_->Call(
        kCalculatorServiceName,
        kAddMethodName,
        std::move(payload),
        options
    );

    if(response.meta.status_code!=protocol::StatusCode::Ok){
        throw std::runtime_error(response.meta.error_text);
    }

    AddResponse result;
    if(!result.ParseFromString(response.payload)){
        throw std::runtime_error(
            "invalid calculator Add response"
        );
    }

    return result;
}

}
