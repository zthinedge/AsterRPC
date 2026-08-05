#include "asterrpc/health/HealthService.h"

#include "asterrpc/rpc/RpcServer.h"

#include <stdexcept>

namespace asterrpc::health{

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
