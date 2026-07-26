#include "minirpc/health/HealthService.h"

#include "minirpc/rpc/RpcServer.h"

#include <stdexcept>

namespace minirpc::health{

void HealthService::RegisterTo(rpc::RpcServer* server){
    if(server==nullptr){
        throw std::invalid_argument("health service RpcServer is null");
    }

    server->RegisterMethod(
        ServiceName(),
        MethodName(),
        [](const std::string& request){
            return Check(request);
        }
    );
}

std::string HealthService::Check(const std::string&){
    return ServingPayload();
}

}
