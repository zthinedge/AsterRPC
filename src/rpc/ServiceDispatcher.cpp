#include "asterrpc/rpc/ServiceDispatcher.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{
namespace{

protocol::RpcMessage MakeResponse(
    const protocol::RpcMessage& request
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.codec=request.codec;
    response.request_id=request.request_id;
    return response;
}

void SetError(
    protocol::RpcMessage* response,
    protocol::StatusCode status_code,
    const std::string& error_text
){
    response->meta.status_code=status_code;
    response->meta.error_text=error_text;
}

}

void ServiceDispatcher::RegisterMethod(
    std::string service_name,
    std::string method_name,
    MethodHandler handler
){
    if(service_name.empty()){
        throw std::invalid_argument("rpc service name is empty");
    }

    if(method_name.empty()){
        throw std::invalid_argument("rpc method name is empty");
    }

    if(!handler){
        throw std::invalid_argument("rpc method handler is empty");
    }

    MethodMap& methods=services_[std::move(service_name)];
    auto result=methods.emplace(
        std::move(method_name),
        std::move(handler)
    );

    if(!result.second){
        throw std::invalid_argument("rpc method already registered");
    }
}

protocol::RpcMessage ServiceDispatcher::Dispatch(
    const protocol::RpcMessage& request
)const{
    protocol::RpcMessage response=MakeResponse(request);

    auto service=services_.find(request.meta.service_name);
    if(service==services_.end()){
        SetError(
            &response,
            protocol::StatusCode::ServiceNotFound,
            "rpc service not found"
        );
        return response;
    }

    auto method=service->second.find(request.meta.method_name);
    if(method==service->second.end()){
        SetError(
            &response,
            protocol::StatusCode::MethodNotFound,
            "rpc method not found"
        );
        return response;
    }

    try{
        response.payload=method->second(request.payload);
    }catch(const std::exception& error){
        SetError(
            &response,
            protocol::StatusCode::InvokeError,
            error.what()
        );
    }catch(...){
        SetError(
            &response,
            protocol::StatusCode::InvokeError,
            "rpc method invocation failed"
        );
    }

    return response;
}

}
