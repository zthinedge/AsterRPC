#include "CalculatorService.h"

#include "asterrpc/rpc/RpcServer.h"

#include <stdexcept>

namespace asterrpc::example::calculator{

CalculatorServiceAdapter::CalculatorServiceAdapter(
    CalculatorService* service
):service_(service){
    if(service_==nullptr){
        throw std::invalid_argument("calculator service is null");
    }
}

void CalculatorServiceAdapter::RegisterTo(
    rpc::RpcServer* server
)const{
    if(server==nullptr){
        throw std::invalid_argument("rpc server is null");
    }

    CalculatorService* service=service_;
    server->RegisterMethod(
        kCalculatorServiceName,
        kAddMethodName,
        [service](const std::string& payload){
            AddRequest request;
            if(!request.ParseFromString(payload)){
                throw std::runtime_error(
                    "invalid calculator Add request"
                );
            }

            AddResponse response;
            service->Add(request,&response);

            std::string output;
            if(!response.SerializeToString(&output)){
                throw std::runtime_error(
                    "failed to serialize calculator Add response"
                );
            }

            return output;
        }
    );
}

}
