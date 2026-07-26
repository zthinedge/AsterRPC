#include "calculator.pb.h"
#include "minirpc/gateway/HttpMessage.h"
#include "minirpc/gateway/ProtobufHttpGateway.h"
#include "minirpc/gateway/RpcChannel.h"
#include "minirpc/net/Buffer.h"
#include "minirpc/trace/TraceContext.h"

#include <google/protobuf/descriptor.h>
#include <nlohmann/json.hpp>

#include <cassert>
#include <chrono>
#include <string>
#include <utility>

using namespace minirpc;
using namespace minirpc::example::calculator;

namespace{

const google::protobuf::ServiceDescriptor* CalculatorDescriptor(){
    return AddRequest::descriptor()->file()->FindServiceByName(
        "CalculatorService"
    );
}

class FakeRpcChannel:public gateway::RpcChannel{
public:
    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions options,
        Completion completion
    )override{
        service=std::move(service_name);
        method=std::move(method_name);
        timeout=options.timeout;

        const trace::TraceContext* context=
            trace::CurrentTraceContext();
        assert(context!=nullptr);
        trace_id=context->trace_id;

        protocol::RpcMessage response;
        response.message_type=protocol::MessageType::Response;
        response.meta.status_code=status;
        response.meta.error_text=error;

        if(status==protocol::StatusCode::Ok){
            AddRequest request;
            assert(request.ParseFromString(payload));
            AddResponse result;
            result.set_result(request.a()+request.b());
            assert(result.SerializeToString(&response.payload));
        }
        completion(std::move(response));
    }

    protocol::StatusCode status=protocol::StatusCode::Ok;
    std::string error;
    std::string service;
    std::string method;
    std::string trace_id;
    std::chrono::microseconds timeout{0};
};

gateway::HttpRequest MakeRequest(std::string body){
    gateway::HttpRequest request;
    request.method="POST";
    request.target="/rpc/CalculatorService/Add";
    request.version="HTTP/1.1";
    request.headers.emplace("content-type","application/json");
    request.body=std::move(body);
    return request;
}

gateway::HttpResponse Handle(
    gateway::ProtobufHttpGateway* gateway,
    gateway::HttpRequest request
){
    gateway::HttpResponse response;
    bool completed=false;
    gateway->Handle(
        std::move(request),
        [&response,&completed](gateway::HttpResponse result){
            response=std::move(result);
            completed=true;
        }
    );
    assert(completed);
    return response;
}

void TestIncrementalHttpParser(){
    net::Buffer buffer;
    buffer.Append(
        "POST /rpc/CalculatorService/Add HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 15\r\n\r\n"
        "{\"a\":20"
    );

    gateway::HttpRequest request;
    auto parsed=gateway::ParseHttpRequest(&buffer,&request);
    assert(parsed.status==
           gateway::HttpParseStatus::NeedMoreData);

    buffer.Append(",\"b\":22}");
    parsed=gateway::ParseHttpRequest(&buffer,&request);
    assert(parsed.status==gateway::HttpParseStatus::Complete);
    assert(request.method=="POST");
    assert(request.target=="/rpc/CalculatorService/Add");
    assert(request.Header("CONTENT-TYPE")=="application/json");
    assert(request.body==R"({"a":20,"b":22})");
    assert(buffer.ReadableBytes()==0);

    gateway::HttpResponse response;
    response.status=201;
    response.reason="Created";
    response.body=R"({"ok":true})";
    std::string bytes=gateway::SerializeHttpResponse(response);
    assert(bytes.find("HTTP/1.1 201 Created\r\n")==0);
    assert(bytes.find("Content-Length: 11\r\n")!=std::string::npos);
    assert(bytes.find("Connection: close\r\n")!=std::string::npos);
}

void TestDynamicProtobufCallAndTrace(){
    FakeRpcChannel channel;
    gateway::ProtobufHttpGateway gateway(&channel);
    gateway.RegisterService(CalculatorDescriptor());

    auto request=MakeRequest(R"({"a":20,"b":22})");
    request.headers.emplace("x-trace-id","gateway-test-trace");
    gateway::HttpResponse response=Handle(&gateway,std::move(request));

    assert(response.status==200);
    assert(
        response.headers.at("X-Trace-Id")=="gateway-test-trace"
    );
    assert(channel.trace_id=="gateway-test-trace");
    assert(
        channel.service==
        "minirpc.example.calculator.CalculatorService"
    );
    assert(channel.method=="Add");
    assert(channel.timeout==std::chrono::seconds(2));

    nlohmann::json json=nlohmann::json::parse(response.body);
    assert(json.at("result")==42);
}

void TestHttpAndRpcErrors(){
    FakeRpcChannel channel;
    gateway::ProtobufHttpGateway gateway(&channel);
    gateway.RegisterService(CalculatorDescriptor());

    auto request=MakeRequest("{}");
    request.method="GET";
    auto response=Handle(&gateway,std::move(request));
    assert(response.status==405);
    assert(response.headers.at("Allow")=="POST");

    response=Handle(&gateway,MakeRequest("{broken"));
    assert(response.status==400);

    request=MakeRequest("{}");
    request.headers.at("content-type")="text/plain";
    response=Handle(&gateway,std::move(request));
    assert(response.status==415);

    request=MakeRequest("{}");
    request.target="/rpc/MissingService/Add";
    response=Handle(&gateway,std::move(request));
    assert(response.status==404);

    request=MakeRequest("{}");
    request.target="/rpc/CalculatorService/MissingMethod";
    response=Handle(&gateway,std::move(request));
    assert(response.status==404);

    channel.status=protocol::StatusCode::Timeout;
    channel.error="deadline exceeded";
    response=Handle(&gateway,MakeRequest(R"({"a":1,"b":2})"));
    assert(response.status==504);

    channel.status=protocol::StatusCode::ConnectionFailed;
    channel.error="no provider";
    response=Handle(&gateway,MakeRequest(R"({"a":1,"b":2})"));
    assert(response.status==503);

    channel.status=protocol::StatusCode::DecodeError;
    channel.error="invalid response";
    response=Handle(&gateway,MakeRequest(R"({"a":1,"b":2})"));
    assert(response.status==502);

    channel.status=protocol::StatusCode::InternalError;
    channel.error="backend failure";
    response=Handle(&gateway,MakeRequest(R"({"a":1,"b":2})"));
    assert(response.status==500);

    request=MakeRequest(R"({"a":1,"b":2})");
    request.headers.emplace("x-trace-id","bad trace id");
    response=Handle(&gateway,std::move(request));
    assert(response.status==400);
}

}

int main(){
    assert(CalculatorDescriptor()!=nullptr);
    TestIncrementalHttpParser();
    TestDynamicProtobufCallAndTrace();
    TestHttpAndRpcErrors();
    return 0;
}
