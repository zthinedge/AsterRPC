#pragma once

#include "calculator.pb.h"

namespace asterrpc::rpc{
class RpcServer;
}

namespace asterrpc::example::calculator{

inline constexpr const char* kCalculatorServiceName=
    "asterrpc.example.calculator.CalculatorService";
inline constexpr const char* kAddMethodName="Add";

class CalculatorService{
public:
    virtual ~CalculatorService()=default;

    virtual void Add(
        const AddRequest& request,
        AddResponse* response
    )=0;
};

class CalculatorServiceAdapter{
public:
    explicit CalculatorServiceAdapter(CalculatorService* service);

    void RegisterTo(rpc::RpcServer* server)const;

private:
    CalculatorService* service_;
};

}
