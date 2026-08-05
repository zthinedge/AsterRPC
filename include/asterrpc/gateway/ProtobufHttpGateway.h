#pragma once

#include "asterrpc/gateway/HttpMessage.h"

#include <chrono>
#include <functional>
#include <memory>

namespace google::protobuf{
class ServiceDescriptor;
}

namespace asterrpc::gateway{

class RpcChannel;

struct ProtobufHttpGatewayOptions{
    std::chrono::milliseconds default_timeout{2000};
};

class ProtobufHttpGateway{
public:
    using Completion=std::function<void(HttpResponse)>;

    explicit ProtobufHttpGateway(
        RpcChannel* channel,
        ProtobufHttpGatewayOptions options={}
    );
    ~ProtobufHttpGateway();

    ProtobufHttpGateway(const ProtobufHttpGateway&)=delete;
    ProtobufHttpGateway& operator=(
        const ProtobufHttpGateway&
    )=delete;

    void RegisterService(
        const google::protobuf::ServiceDescriptor* service
    );

    void Handle(HttpRequest request,Completion completion);

private:
    class State;
    std::shared_ptr<State> state_;
};

}
