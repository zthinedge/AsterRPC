#include "asterrpc/rpc/ServiceDispatcher.h"
#include "rpc/detail/CallSupport.h"

#include <stdexcept>
#include <utility>

namespace asterrpc::rpc{

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
    protocol::RpcMessage response=detail::MakeResponse(request);

    auto service=services_.find(request.meta.service_name);
    if(service==services_.end()){
        return detail::MakeErrorResponse(
            request,
            protocol::StatusCode::ServiceNotFound,
            "rpc service not found"
        );
    }

    auto method=service->second.find(request.meta.method_name);
    if(method==service->second.end()){
        return detail::MakeErrorResponse(
            request,
            protocol::StatusCode::MethodNotFound,
            "rpc method not found"
        );
    }

    try{
        response.payload=method->second(request.payload);
    }catch(const std::exception& error){
        return detail::MakeErrorResponse(
            request,
            protocol::StatusCode::InvokeError,
            error.what()
        );
    }catch(...){
        return detail::MakeErrorResponse(
            request,
            protocol::StatusCode::InvokeError,
            "rpc method invocation failed"
        );
    }

    return response;
}

}
