#include "minirpc/gateway/HttpGatewayServer.h"

#include "minirpc/gateway/AdminApi.h"
#include "minirpc/gateway/RpcChannel.h"
#include "minirpc/net/Buffer.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/net/InetAddress.h"
#include "minirpc/net/TcpConnection.h"
#include "minirpc/net/TcpServer.h"
#include "minirpc/trace/TraceContext.h"

#include <nlohmann/json.hpp>

#include <stdexcept>
#include <utility>

namespace minirpc::gateway{

class HttpGatewayServer::State:
    public std::enable_shared_from_this<State>{
public:
    State(
        net::EventLoop* loop,
        const net::InetAddress& address,
        RpcChannel* channel,
        ProtobufHttpGatewayOptions gateway_options,
        HttpParserLimits parser_limits
    ):loop_(loop),
      server_(loop,address),
      gateway_(channel,gateway_options),
      parser_limits_(parser_limits){
        if(loop_==nullptr){
            throw std::invalid_argument(
                "HTTP gateway EventLoop is null"
            );
        }
        if(parser_limits_.max_header_bytes==0||
           parser_limits_.max_body_bytes==0){
            throw std::invalid_argument(
                "HTTP parser limits must be positive"
            );
        }
    }

    void RegisterService(
        const google::protobuf::ServiceDescriptor* service
    ){
        gateway_.RegisterService(service);
    }

    void SetAdminDataSource(AdminDataSource* data_source){
        if(started_){
            throw std::logic_error(
                "admin data source must be set before start"
            );
        }
        admin_api_=std::make_unique<AdminApi>(data_source);
    }

    void Start(){
        if(started_){
            throw std::logic_error(
                "HTTP gateway server already started"
            );
        }
        started_=true;

        std::weak_ptr<State> weak=shared_from_this();
        server_.SetMessageCallback(
            [weak](
                net::TcpConnection* connection,
                net::Buffer* buffer
            ){
                if(auto self=weak.lock()){
                    self->HandleMessage(connection,buffer);
                }
            }
        );
        server_.Start();
    }

private:
    void HandleMessage(
        net::TcpConnection* connection,
        net::Buffer* buffer
    ){
        HttpRequest request;
        HttpParseResult parsed=ParseHttpRequest(
            buffer,
            &request,
            parser_limits_
        );
        if(parsed.status==HttpParseStatus::NeedMoreData){
            return;
        }
        if(parsed.status==HttpParseStatus::Error){
            buffer->RetrieveAll();
            trace::TraceContext context=trace::CreateRootTrace();
            HttpResponse response;
            response.status=400;
            response.reason="Bad Request";
            response.headers.emplace(
                "X-Trace-Id",
                context.trace_id
            );
            response.body=nlohmann::json{
                {"error","invalid_http_request"},
                {"message",parsed.error},
                {"trace_id",context.trace_id}
            }.dump();
            SendResponse(connection,std::move(response));
            return;
        }

        if(admin_api_!=nullptr&&AdminApi::Matches(request.target)){
            SendResponse(
                connection,
                admin_api_->Handle(request)
            );
            return;
        }

        std::weak_ptr<State> weak=shared_from_this();
        std::weak_ptr<const int> connection_lifetime=
            connection->LifetimeToken();
        gateway_.Handle(
            std::move(request),
            [
                weak,
                connection,
                connection_lifetime
            ](HttpResponse response)mutable{
                if(auto self=weak.lock()){
                    self->loop_->QueueInLoop(
                        [
                            weak,
                            connection,
                            connection_lifetime,
                            response=std::move(response)
                        ]()mutable{
                            if(auto current=weak.lock();
                               current!=nullptr&&
                               !connection_lifetime.expired()){
                                current->SendResponse(
                                    connection,
                                    std::move(response)
                                );
                            }
                        }
                    );
                }
            }
        );
    }

    void SendResponse(
        net::TcpConnection* connection,
        HttpResponse response
    ){
        connection->Send(SerializeHttpResponse(response));
        connection->Shutdown();
    }

    net::EventLoop* loop_;
    net::TcpServer server_;
    ProtobufHttpGateway gateway_;
    std::unique_ptr<AdminApi> admin_api_;
    HttpParserLimits parser_limits_;
    bool started_=false;
};

HttpGatewayServer::HttpGatewayServer(
    net::EventLoop* loop,
    const net::InetAddress& address,
    RpcChannel* channel,
    ProtobufHttpGatewayOptions gateway_options,
    HttpParserLimits parser_limits
):state_(std::make_shared<State>(
      loop,
      address,
      channel,
      gateway_options,
      parser_limits
  )){}

HttpGatewayServer::~HttpGatewayServer()=default;

void HttpGatewayServer::RegisterService(
    const google::protobuf::ServiceDescriptor* service
){
    state_->RegisterService(service);
}

void HttpGatewayServer::SetAdminDataSource(
    AdminDataSource* data_source
){
    state_->SetAdminDataSource(data_source);
}

void HttpGatewayServer::Start(){
    state_->Start();
}

}
