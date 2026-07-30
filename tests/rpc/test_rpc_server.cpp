#include "minirpc/health/HealthService.h"
#include "minirpc/net/Buffer.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/net/InetAddress.h"
#include "minirpc/protocol/RpcCodec.h"
#include "minirpc/rpc/RpcServer.h"
#include "minirpc/rpc/ServiceDispatcher.h"
#include "minirpc/trace/TraceContext.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>

using namespace minirpc;

namespace{

void Check(bool condition){
    if(!condition){
        std::abort();
    }
}

std::uint16_t FindFreePort(){
    int fd=::socket(AF_INET,SOCK_STREAM,0);
    Check(fd!=-1);

    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    addr.sin_port=0;

    Check(::bind(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr)
    )==0);

    socklen_t len=sizeof(addr);
    Check(::getsockname(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        &len
    )==0);

    std::uint16_t port=ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

protocol::RpcMessage MakeRequest(
    std::uint64_t request_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& payload
){
    protocol::RpcMessage request;
    request.message_type=protocol::MessageType::Request;
    request.request_id=request_id;
    request.meta.service_name=service_name;
    request.meta.method_name=method_name;
    request.payload=payload;
    return request;
}

void SendAll(int fd,const std::string& data){
    std::size_t sent=0;

    while(sent<data.size()){
        ssize_t size=::send(
            fd,
            data.data()+sent,
            data.size()-sent,
            MSG_NOSIGNAL
        );

        if(size>0){
            sent+=static_cast<std::size_t>(size);
            continue;
        }

        if(size==-1&&errno==EINTR){
            continue;
        }

        Check(false);
    }
}

int Connect(std::uint16_t port){
    int fd=::socket(AF_INET,SOCK_STREAM,0);
    Check(fd!=-1);

    timeval timeout{};
    timeout.tv_sec=2;
    Check(::setsockopt(
        fd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    )==0);

    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    addr.sin_port=htons(port);

    Check(::connect(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr)
    )==0);

    return fd;
}

protocol::RpcMessage ReadOne(
    int fd,
    net::Buffer* buffer
){
    protocol::RpcCodec codec;

    while(true){
        protocol::RpcMessage response;
        std::string error;
        protocol::DecodeStatus status=codec.DecodeOne(
            buffer,
            &response,
            &error
        );

        if(status==protocol::DecodeStatus::Ok){
            return response;
        }

        Check(status==protocol::DecodeStatus::NeedMoreData);

        char data[4096];
        ssize_t size=::recv(fd,data,sizeof(data),0);

        if(size==-1&&errno==EINTR){
            continue;
        }

        Check(size>0);
        buffer->Append(data,static_cast<std::size_t>(size));
    }
}

void TestDispatcher(){
    rpc::ServiceDispatcher dispatcher;
    dispatcher.RegisterMethod(
        "CalculatorService",
        "Add",
        [](const std::string& payload){
            return "result:"+payload;
        }
    );

    protocol::RpcMessage request=MakeRequest(
        41,
        "CalculatorService",
        "Add",
        "1+2"
    );

    protocol::RpcMessage response=dispatcher.Dispatch(request);
    Check(response.message_type==protocol::MessageType::Response);
    Check(response.request_id==request.request_id);
    Check(response.meta.status_code==protocol::StatusCode::Ok);
    Check(response.payload=="result:1+2");

    request.meta.service_name="UnknownService";
    response=dispatcher.Dispatch(request);
    Check(response.request_id==request.request_id);
    Check(response.meta.status_code==
           protocol::StatusCode::ServiceNotFound);

    request.meta.service_name="CalculatorService";
    request.meta.method_name="UnknownMethod";
    response=dispatcher.Dispatch(request);
    Check(response.request_id==request.request_id);
    Check(response.meta.status_code==
           protocol::StatusCode::MethodNotFound);
}

void TestRpcServer(){
    std::uint16_t port=FindFreePort();
    std::promise<net::EventLoop*>server_ready;
    std::promise<rpc::RpcServer*>rpc_server_ready;
    std::promise<trace::TraceContext>handler_trace;
    std::promise<std::thread::id>server_thread_id;
    std::promise<std::thread::id>handler_thread_id;

    std::thread server_thread([
        port,
        &server_ready,
        &rpc_server_ready,
        &handler_trace,
        &server_thread_id,
        &handler_thread_id
    ](){
        net::EventLoop loop;
        net::InetAddress address("127.0.0.1",port);
        rpc::RpcServerOptions options;
        options.tcp.io_threads=2;
        options.tcp.io_load_balance=
            net::IoLoopLoadBalance::LeastConnections;
        options.business_threads=2;
        rpc::RpcServer server(&loop,address,options);
        server_thread_id.set_value(std::this_thread::get_id());

        server.RegisterMethod(
            "EchoService",
            "Echo",
            [
                &handler_trace,
                &handler_thread_id
            ](const std::string& payload){
                const trace::TraceContext* context=
                    trace::CurrentTraceContext();
                Check(context!=nullptr);
                handler_trace.set_value(*context);
                handler_thread_id.set_value(std::this_thread::get_id());
                return "reply:"+payload;
            }
        );

        server.Start();
        server_ready.set_value(&loop);
        rpc_server_ready.set_value(&server);
        loop.Loop();
    });

    net::EventLoop*loop=server_ready.get_future().get();
    rpc::RpcServer*server=rpc_server_ready.get_future().get();
    int fd=Connect(port);

    protocol::RpcCodec codec;
    protocol::RpcMessage first_request=
        MakeRequest(101,"EchoService","Echo","hello");
    first_request.meta.trace_id=
        "00112233445566778899aabbccddeeff";
    first_request.meta.span_id="0123456789abcdef";
    std::string first=codec.Encode(first_request);
    std::string second=codec.Encode(
        MakeRequest(102,"UnknownService","Echo","")
    );
    std::string third=codec.Encode(
        MakeRequest(103,"EchoService","UnknownMethod","")
    );

    std::size_t split=first.size()/2;
    SendAll(fd,first.substr(0,split));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    SendAll(fd,first.substr(split)+second+third);

    net::Buffer response_buffer;

    std::unordered_map<std::uint64_t,protocol::RpcMessage> responses;
    for(std::size_t index=0;index<3;++index){
        protocol::RpcMessage received=ReadOne(fd,&response_buffer);
        responses.emplace(received.request_id,std::move(received));
    }

    const protocol::RpcMessage& first_response=responses.at(101);
    Check(first_response.meta.status_code==protocol::StatusCode::Ok);
    Check(first_response.payload=="reply:hello");
    Check(first_response.meta.trace_id==first_request.meta.trace_id);
    Check(first_response.meta.parent_span_id==first_request.meta.span_id);
    Check(!first_response.meta.span_id.empty());
    Check(first_response.meta.span_id!=first_request.meta.span_id);

    trace::TraceContext observed=handler_trace.get_future().get();
    Check(handler_thread_id.get_future().get()!=
           server_thread_id.get_future().get());
    Check(observed.trace_id==first_request.meta.trace_id);
    Check(observed.span_id==first_response.meta.span_id);
    Check(observed.parent_span_id==first_request.meta.span_id);

    Check(responses.at(102).meta.status_code==
           protocol::StatusCode::ServiceNotFound);

    Check(responses.at(103).meta.status_code==
           protocol::StatusCode::MethodNotFound);

    SendAll(
        fd,
        codec.Encode(MakeRequest(
            200,
            health::HealthService::ServiceName(),
            health::HealthService::MethodName(),
            {}
        ))
    );

    protocol::RpcMessage response=ReadOne(fd,&response_buffer);
    Check(response.request_id==200);
    Check(response.meta.status_code==protocol::StatusCode::Ok);
    Check(response.payload==
           health::HealthService::ServingPayload());

    protocol::RpcMessage expired_request=MakeRequest(
        104,
        "EchoService",
        "Echo",
        "expired"
    );
    expired_request.meta.deadline_us=1;
    SendAll(fd,codec.Encode(expired_request));

    response=ReadOne(fd,&response_buffer);
    Check(response.request_id==104);
    Check(response.meta.status_code==protocol::RpcError::Timeout);
    Check(response.payload.empty());

    auto metrics=server->GetMetrics();
    Check(metrics.total_requests==5);
    Check(metrics.successful_requests==2);
    Check(metrics.failed_requests==3);
    Check(metrics.timeout_requests==1);
    Check(metrics.inflight_requests==0);
    Check(metrics.active_connections==1);
    Check(metrics.latency_samples==5);
    Check(server->IoThreadCount()==2);
    Check(server->BusinessThreadCount()==2);
    auto connection_counts=server->IoConnectionCounts();
    Check(connection_counts.size()==2);
    Check(connection_counts[0]+connection_counts[1]==1);

    auto echo_metrics=server->GetMethodMetrics(
        "EchoService",
        "Echo"
    );
    Check(echo_metrics.total_requests==2);
    Check(echo_metrics.successful_requests==1);
    Check(echo_metrics.timeout_requests==1);
    Check(server->GetAllMethodMetrics().size()==4);

    std::string invalid=codec.Encode(
        MakeRequest(105,"EchoService","Echo","bad")
    );
    invalid[0]=0;
    SendAll(fd,invalid);

    char data=0;
    Check(::recv(fd,&data,1,0)==0);

    ::close(fd);
    loop->Stop();
    server_thread.join();
}

}

int main(){
    TestDispatcher();
    TestRpcServer();
    return 0;
}
