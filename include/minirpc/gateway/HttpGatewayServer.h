#pragma once

#include "minirpc/gateway/HttpMessage.h"
#include "minirpc/gateway/ProtobufHttpGateway.h"

#include <memory>

namespace google::protobuf{
class ServiceDescriptor;
}

namespace minirpc::gateway{

class RpcChannel;

}

namespace minirpc::net{
class EventLoop;
class InetAddress;
}

namespace minirpc::gateway{

class HttpGatewayServer{
public:
    HttpGatewayServer(
        net::EventLoop* loop,
        const net::InetAddress& address,
        RpcChannel* channel,
        ProtobufHttpGatewayOptions gateway_options={},
        HttpParserLimits parser_limits={}
    );
    ~HttpGatewayServer();

    HttpGatewayServer(const HttpGatewayServer&)=delete;
    HttpGatewayServer& operator=(const HttpGatewayServer&)=delete;

    void RegisterService(
        const google::protobuf::ServiceDescriptor* service
    );
    void Start();

private:
    class State;
    std::shared_ptr<State> state_;
};

}
