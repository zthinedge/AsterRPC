#pragma once

#include "asterrpc/gateway/HttpMessage.h"
#include "asterrpc/gateway/ProtobufHttpGateway.h"

#include <memory>

namespace google::protobuf{
class ServiceDescriptor;
}

namespace asterrpc::gateway{

class AdminDataSource;
class RpcChannel;

}

namespace asterrpc::net{
class EventLoop;
class InetAddress;
}

namespace asterrpc::gateway{

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
    void SetAdminDataSource(AdminDataSource* data_source);
    void Start();

private:
    class State;
    std::shared_ptr<State> state_;
};

}
