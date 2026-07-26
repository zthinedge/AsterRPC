#include "CalculatorService.h"
#include "CalculatorStub.h"
#include "minirpc/net/Buffer.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/net/InetAddress.h"
#include "minirpc/protocol/RpcCodec.h"
#include "minirpc/protocol/RpcMessage.h"
#include "minirpc/rpc/PendingCalls.h"
#include "minirpc/trace/TraceContext.h"
#include "minirpc/rpc/RpcClient.h"
#include "minirpc/rpc/RpcServer.h"

#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <future>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace minirpc;
using namespace minirpc::example::calculator;

namespace{

class CalculatorServiceImpl:public CalculatorService{
public:
    void Add(
        const AddRequest& request,
        AddResponse* response
    )override{
        response->set_result(request.a()+request.b());
    }
};

std::uint64_t CurrentTimeMicros(){
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

std::uint16_t FindFreePort(){
    int fd=::socket(AF_INET,SOCK_STREAM,0);
    assert(fd!=-1);

    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    addr.sin_port=0;

    assert(::bind(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr)
    )==0);

    socklen_t len=sizeof(addr);
    assert(::getsockname(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        &len
    )==0);

    std::uint16_t port=ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

protocol::RpcMessage MakeResponse(
    std::uint64_t request_id,
    const std::string& payload
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.request_id=request_id;
    response.payload=payload;
    return response;
}

void SendAll(int fd,const std::string& bytes){
    std::size_t sent=0;

    while(sent<bytes.size()){
        ssize_t size=::send(
            fd,
            bytes.data()+sent,
            bytes.size()-sent,
            MSG_NOSIGNAL
        );

        if(size==-1&&errno==EINTR){
            continue;
        }

        assert(size>0);
        sent+=static_cast<std::size_t>(size);
    }
}

bool TrySendAll(int fd,const std::string& bytes){
    std::size_t sent=0;

    while(sent<bytes.size()){
        ssize_t size=::send(
            fd,
            bytes.data()+sent,
            bytes.size()-sent,
            MSG_NOSIGNAL
        );

        if(size==-1&&errno==EINTR){
            continue;
        }

        if(size<=0){
            return false;
        }

        sent+=static_cast<std::size_t>(size);
    }

    return true;
}

int CreateListener(std::uint16_t port,std::promise<void>* ready){
    int listen_fd=::socket(AF_INET,SOCK_STREAM,0);
    assert(listen_fd!=-1);

    int reuse=1;
    assert(::setsockopt(
        listen_fd,
        SOL_SOCKET,
        SO_REUSEADDR,
        &reuse,
        sizeof(reuse)
    )==0);

    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    addr.sin_port=htons(port);

    assert(::bind(
        listen_fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr)
    )==0);
    assert(::listen(listen_fd,1)==0);
    ready->set_value();
    return listen_fd;
}

std::vector<protocol::RpcMessage> ReadRequests(
    int fd,
    std::size_t count,
    net::Buffer* buffer
){
    protocol::RpcCodec codec;
    std::vector<protocol::RpcMessage> requests;

    while(requests.size()<count){
        protocol::RpcMessage request;
        std::string error;
        protocol::DecodeStatus status=codec.DecodeOne(
            buffer,
            &request,
            &error
        );

        if(status==protocol::DecodeStatus::Ok){
            requests.push_back(std::move(request));
            continue;
        }

        assert(status==protocol::DecodeStatus::NeedMoreData);

        char data[4096];
        ssize_t size=::recv(fd,data,sizeof(data),0);

        if(size==-1&&errno==EINTR){
            continue;
        }

        assert(size>0);
        buffer->Append(data,static_cast<std::size_t>(size));
    }

    return requests;
}

bool ReadOneRequest(
    int fd,
    net::Buffer* buffer,
    protocol::RpcMessage* request
){
    protocol::RpcCodec codec;

    while(true){
        std::string error;
        protocol::DecodeStatus status=codec.DecodeOne(
            buffer,
            request,
            &error
        );

        if(status==protocol::DecodeStatus::Ok){
            return true;
        }

        assert(status==protocol::DecodeStatus::NeedMoreData);

        char data[4096];
        ssize_t size=::recv(fd,data,sizeof(data),0);

        if(size==-1&&errno==EINTR){
            continue;
        }

        if(size<=0){
            return false;
        }

        buffer->Append(data,static_cast<std::size_t>(size));
    }
}

void RunOutOfOrderServer(
    std::uint16_t port,
    std::promise<void>* ready
){
    int listen_fd=CreateListener(port,ready);

    int fd=::accept(listen_fd,nullptr,nullptr);
    assert(fd!=-1);

    net::Buffer buffer;
    protocol::RpcCodec codec;
    auto requests=ReadRequests(fd,2,&buffer);

    protocol::RpcMessage second=MakeResponse(
        requests[1].request_id,
        requests[1].payload
    );
    protocol::RpcMessage first=MakeResponse(
        requests[0].request_id,
        requests[0].payload
    );

    SendAll(fd,codec.Encode(second));
    SendAll(fd,codec.Encode(first));

    ReadRequests(fd,2,&buffer);
    ::close(fd);
    ::close(listen_fd);
}

void RunTimeoutServer(
    std::uint16_t port,
    std::promise<void>* ready,
    std::promise<std::vector<protocol::RpcMessage>>* received
){
    int listen_fd=CreateListener(port,ready);
    int fd=::accept(listen_fd,nullptr,nullptr);
    assert(fd!=-1);

    net::Buffer buffer;
    received->set_value(ReadRequests(fd,2,&buffer));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ::close(fd);
    ::close(listen_fd);
}

void RunRetrySuccessServer(
    std::uint16_t port,
    std::promise<void>* ready,
    std::promise<std::vector<protocol::RpcMessage>>* received
){
    int listen_fd=CreateListener(port,ready);
    int fd=::accept(listen_fd,nullptr,nullptr);
    assert(fd!=-1);

    net::Buffer buffer;
    protocol::RpcCodec codec;
    std::vector<protocol::RpcMessage> requests;

    for(int attempt=0;attempt<3;attempt++){
        protocol::RpcMessage request;
        assert(ReadOneRequest(fd,&buffer,&request));
        requests.push_back(request);

        protocol::RpcMessage response=MakeResponse(
            request.request_id,
            request.payload
        );

        if(attempt<2){
            response.meta.status_code=
                protocol::RpcError::InternalError;
            response.meta.error_text="retryable failure";
        }

        SendAll(fd,codec.Encode(response));
    }

    received->set_value(std::move(requests));
    ::close(fd);
    ::close(listen_fd);
}

void RunRetryDeadlineServer(
    std::uint16_t port,
    std::promise<void>* ready,
    std::promise<std::vector<protocol::RpcMessage>>* received
){
    int listen_fd=CreateListener(port,ready);
    int fd=::accept(listen_fd,nullptr,nullptr);
    assert(fd!=-1);

    timeval timeout{};
    timeout.tv_sec=2;
    assert(::setsockopt(
        fd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    )==0);

    net::Buffer buffer;
    protocol::RpcCodec codec;
    std::vector<protocol::RpcMessage> requests;

    while(true){
        protocol::RpcMessage request;
        if(!ReadOneRequest(fd,&buffer,&request)){
            break;
        }

        requests.push_back(request);
        std::this_thread::sleep_for(std::chrono::milliseconds(15));

        protocol::RpcMessage response=MakeResponse(
            request.request_id,
            ""
        );
        response.meta.status_code=protocol::RpcError::InternalError;
        response.meta.error_text="retryable failure";

        if(!TrySendAll(fd,codec.Encode(response))){
            break;
        }
    }

    received->set_value(std::move(requests));
    ::close(fd);
    ::close(listen_fd);
}

void RunClient(
    std::uint16_t port,
    std::promise<rpc::RpcClient*>* client_ready,
    std::promise<net::EventLoop*>* loop_ready
){
    net::EventLoop loop;
    net::InetAddress address("127.0.0.1",port);
    rpc::RpcClient client(&loop,address);

    client.SetConnectionCallback([&](){
        client_ready->set_value(&client);
    });

    loop_ready->set_value(&loop);
    client.Connect();
    loop.Loop();
}

void TestPendingCalls(){
    rpc::PendingCalls calls;
    auto first=calls.Add(1);
    auto second=calls.Add(2);
    std::promise<protocol::RpcMessage> callback_result;

    calls.Add(3,[&callback_result](protocol::RpcMessage response){
        callback_result.set_value(std::move(response));
    });

    assert(calls.Size()==3);
    assert(calls.Complete(MakeResponse(2,"second")));
    assert(calls.Size()==2);
    assert(calls.Complete(MakeResponse(3,"callback")));
    assert(calls.Complete(MakeResponse(1,"first")));
    assert(calls.Size()==0);

    assert(first.get().payload=="first");
    assert(second.get().payload=="second");
    assert(callback_result.get_future().get().payload=="callback");
    assert(!calls.Complete(MakeResponse(3,"unknown")));

    auto failed_future=calls.Add(4);
    std::promise<protocol::RpcMessage> failed_callback;
    calls.Add(5,[&failed_callback](protocol::RpcMessage response){
        failed_callback.set_value(std::move(response));
    });

    calls.FailAll(
        protocol::StatusCode::InternalError,
        "connection closed"
    );

    assert(calls.Size()==0);
    assert(failed_future.get().meta.status_code==
           protocol::StatusCode::InternalError);
    assert(failed_callback.get_future().get().meta.status_code==
           protocol::StatusCode::InternalError);

    bool timeout_cancelled=false;
    auto timeout_future=calls.Add(6,1);
    assert(calls.SetTimeoutCancel(6,[&timeout_cancelled](){
        timeout_cancelled=true;
    }));
    assert(calls.Expire(6));
    assert(calls.Size()==0);
    assert(timeout_cancelled);
    assert(timeout_future.get().meta.status_code==
           protocol::RpcError::Timeout);
}

void TestCalculatorRpc(){
    std::uint16_t port=FindFreePort();
    std::promise<net::EventLoop*>server_ready;

    std::thread server_thread([port,&server_ready](){
        net::EventLoop loop;
        net::InetAddress address("127.0.0.1",port);
        rpc::RpcServer server(&loop,address);
        CalculatorServiceImpl service;
        CalculatorServiceAdapter adapter(&service);

        adapter.RegisterTo(&server);
        server.Start();
        server_ready.set_value(&loop);
        loop.Loop();
    });

    net::EventLoop*server_loop=server_ready.get_future().get();
    std::promise<rpc::RpcClient*>client_ready;
    std::promise<net::EventLoop*>client_loop_ready;

    std::thread client_thread(
        [port,&client_ready,&client_loop_ready](){
            net::EventLoop loop;
            net::InetAddress address("127.0.0.1",port);
            rpc::RpcClient client(&loop,address);

            client.SetConnectionCallback([&](){
                client_ready.set_value(&client);
            });

            client_loop_ready.set_value(&loop);
            client.Connect();
            loop.Loop();
        }
    );

    rpc::RpcClient*client=client_ready.get_future().get();
    net::EventLoop*client_loop=client_loop_ready.get_future().get();
    CalculatorStub stub(client);

    AddRequest request;
    request.set_a(20);
    request.set_b(22);
    AddResponse response=stub.Add(request);
    assert(response.result()==42);

    constexpr int call_count=16;
    std::vector<std::thread>callers;
    std::vector<int>results(call_count);

    for(int i=0;i<call_count;++i){
        callers.emplace_back([i,&stub,&results](){
            AddRequest request;
            request.set_a(i);
            request.set_b(i*2);
            results[i]=stub.Add(request).result();
        });
    }

    for(auto&caller:callers){
        caller.join();
    }

    for(int i=0;i<call_count;++i){
        assert(results[i]==i*3);
    }

    client_loop->Stop();
    client_thread.join();
    server_loop->Stop();
    server_thread.join();
}

void TestOutOfOrderAndDisconnect(){
    std::uint16_t port=FindFreePort();
    std::promise<void>server_ready;
    std::thread server_thread(
        RunOutOfOrderServer,
        port,
        &server_ready
    );
    server_ready.get_future().get();

    std::promise<rpc::RpcClient*>client_ready;
    std::promise<net::EventLoop*>loop_ready;
    std::thread client_thread(
        [port,&client_ready,&loop_ready](){
            net::EventLoop loop;
            net::InetAddress address("127.0.0.1",port);
            rpc::RpcClient client(&loop,address);

            client.SetConnectionCallback([&](){
                client_ready.set_value(&client);
            });

            loop_ready.set_value(&loop);
            client.Connect();
            loop.Loop();
        }
    );

    rpc::RpcClient*client=client_ready.get_future().get();
    net::EventLoop*loop=loop_ready.get_future().get();

    auto future=client->FutureCall(
        "TestService",
        "Future",
        "future response"
    );

    std::promise<protocol::RpcMessage>callback_result;
    client->AsyncCall(
        "TestService",
        "Callback",
        "callback response",
        [&callback_result](protocol::RpcMessage response){
            callback_result.set_value(std::move(response));
        }
    );

    auto callback_future=callback_result.get_future();
    assert(callback_future.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(future.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(callback_future.get().payload=="callback response");
    assert(future.get().payload=="future response");

    auto disconnected_future=client->FutureCall(
        "TestService",
        "PendingFuture",
        ""
    );

    std::promise<protocol::RpcMessage>disconnected_callback;
    client->AsyncCall(
        "TestService",
        "PendingCallback",
        "",
        [&disconnected_callback](protocol::RpcMessage response){
            disconnected_callback.set_value(std::move(response));
        }
    );

    auto disconnected_callback_future=
        disconnected_callback.get_future();

    assert(disconnected_future.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(disconnected_callback_future.wait_for(
               std::chrono::seconds(2)
           )==std::future_status::ready);

    assert(disconnected_future.get().meta.status_code==
           protocol::StatusCode::ConnectionFailed);
    assert(disconnected_callback_future.get().meta.status_code==
           protocol::StatusCode::ConnectionFailed);

    loop->Stop();
    client_thread.join();
    server_thread.join();
}

void TestClientTimeout(){
    std::uint16_t port=FindFreePort();
    std::promise<void>server_ready;
    std::promise<std::vector<protocol::RpcMessage>>received;
    auto received_future=received.get_future();
    std::thread server_thread(
        RunTimeoutServer,
        port,
        &server_ready,
        &received
    );
    server_ready.get_future().get();

    std::promise<rpc::RpcClient*>client_ready;
    std::promise<net::EventLoop*>loop_ready;
    std::thread client_thread(
        RunClient,
        port,
        &client_ready,
        &loop_ready
    );

    rpc::RpcClient*client=client_ready.get_future().get();
    net::EventLoop*loop=loop_ready.get_future().get();
    rpc::CallOptions timeout_options;
    timeout_options.timeout=std::chrono::milliseconds(20);

    auto future=client->FutureCall(
        "TimeoutService",
        "Future",
        "",
        timeout_options
    );

    rpc::CallOptions deadline_options;
    deadline_options.timeout=std::chrono::seconds(1);
    deadline_options.deadline_us=CurrentTimeMicros()+20000;

    std::promise<protocol::RpcMessage>callback_result;
    auto callback_future=callback_result.get_future();
    client->AsyncCall(
        "TimeoutService",
        "Callback",
        "",
        [&callback_result](protocol::RpcMessage response){
            callback_result.set_value(std::move(response));
        },
        deadline_options
    );

    assert(future.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(callback_future.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(future.get().meta.status_code==protocol::RpcError::Timeout);
    assert(callback_future.get().meta.status_code==
           protocol::RpcError::Timeout);

    auto metrics=client->GetMetrics();
    assert(metrics.total_requests==2);
    assert(metrics.successful_requests==0);
    assert(metrics.failed_requests==2);
    assert(metrics.timeout_requests==2);
    assert(metrics.inflight_requests==0);

    auto requests=received_future.get();
    assert(requests.size()==2);
    assert(requests[0].meta.deadline_us!=0);
    assert(requests[1].meta.deadline_us!=0);
    assert(!requests[0].meta.trace_id.empty());
    assert(!requests[0].meta.span_id.empty());
    assert(!requests[1].meta.trace_id.empty());
    assert(!requests[1].meta.span_id.empty());

    loop->Stop();
    client_thread.join();
    server_thread.join();
}

void TestRetrySuccess(){
    std::uint16_t port=FindFreePort();
    std::promise<void>server_ready;
    std::promise<std::vector<protocol::RpcMessage>>received;
    auto received_future=received.get_future();
    std::thread server_thread(
        RunRetrySuccessServer,
        port,
        &server_ready,
        &received
    );
    server_ready.get_future().get();

    std::promise<rpc::RpcClient*>client_ready;
    std::promise<net::EventLoop*>loop_ready;
    std::thread client_thread(
        RunClient,
        port,
        &client_ready,
        &loop_ready
    );

    rpc::RpcClient*client=client_ready.get_future().get();
    net::EventLoop*loop=loop_ready.get_future().get();
    rpc::CallOptions options;
    options.idempotent=true;
    options.timeout=std::chrono::milliseconds(500);
    options.max_retries=2;

    trace::TraceContext parent=trace::CreateRootTrace();
    protocol::RpcMessage response;
    {
        trace::TraceScope scope(parent);
        response=client->FutureCall(
            "RetryService",
            "Retry",
            "success after retry",
            options
        ).get();
    }

    assert(response.meta.status_code==protocol::RpcError::Ok);
    assert(response.payload=="success after retry");

    auto requests=received_future.get();
    assert(requests.size()==3);
    assert(requests[0].request_id!=requests[1].request_id);
    assert(requests[1].request_id!=requests[2].request_id);
    assert(requests[0].meta.deadline_us!=0);
    assert(requests[0].meta.deadline_us==
           requests[1].meta.deadline_us);
    assert(requests[1].meta.deadline_us==
           requests[2].meta.deadline_us);
    assert(requests[0].meta.trace_id==parent.trace_id);
    assert(requests[0].meta.parent_span_id==parent.span_id);
    assert(!requests[0].meta.span_id.empty());
    assert(requests[0].meta.span_id!=parent.span_id);
    assert(requests[0].meta.trace_id==
           requests[1].meta.trace_id);
    assert(requests[0].meta.trace_id==
           requests[2].meta.trace_id);
    assert(requests[0].meta.span_id==
           requests[1].meta.span_id);
    assert(requests[0].meta.span_id==
           requests[2].meta.span_id);

    auto metrics=client->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.successful_requests==1);
    assert(metrics.failed_requests==0);
    assert(metrics.retries==2);
    assert(metrics.inflight_requests==0);
    assert(metrics.latency_samples==1);
    auto method_metrics=client->GetMethodMetrics(
        "RetryService",
        "Retry"
    );
    assert(method_metrics.total_requests==1);
    assert(method_metrics.successful_requests==1);
    assert(method_metrics.retries==2);

    loop->Stop();
    client_thread.join();
    server_thread.join();
}

void TestRetryStopsAtDeadline(){
    std::uint16_t port=FindFreePort();
    std::promise<void>server_ready;
    std::promise<std::vector<protocol::RpcMessage>>received;
    auto received_future=received.get_future();
    std::thread server_thread(
        RunRetryDeadlineServer,
        port,
        &server_ready,
        &received
    );
    server_ready.get_future().get();

    std::promise<rpc::RpcClient*>client_ready;
    std::promise<net::EventLoop*>loop_ready;
    std::thread client_thread(
        RunClient,
        port,
        &client_ready,
        &loop_ready
    );

    rpc::RpcClient*client=client_ready.get_future().get();
    net::EventLoop*loop=loop_ready.get_future().get();
    rpc::CallOptions options;
    options.idempotent=true;
    options.timeout=std::chrono::milliseconds(35);
    options.max_retries=100;

    auto start=std::chrono::steady_clock::now();
    protocol::RpcMessage response=client->FutureCall(
        "RetryService",
        "AlwaysFail",
        "",
        options
    ).get();
    auto elapsed=std::chrono::steady_clock::now()-start;

    assert(response.meta.status_code==protocol::RpcError::Timeout);
    assert(elapsed<std::chrono::milliseconds(500));

    auto metrics=client->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.successful_requests==0);
    assert(metrics.failed_requests==1);
    assert(metrics.timeout_requests==1);
    assert(metrics.inflight_requests==0);

    loop->Stop();
    client_thread.join();
    server_thread.join();

    auto requests=received_future.get();
    assert(!requests.empty());
    assert(requests.size()<options.max_retries+1);

    for(const auto& request:requests){
        assert(request.meta.deadline_us==
               requests.front().meta.deadline_us);
    }
}

}

int main(){
    TestPendingCalls();
    TestCalculatorRpc();
    TestOutOfOrderAndDisconnect();
    TestClientTimeout();
    TestRetrySuccess();
    TestRetryStopsAtDeadline();
    return 0;
}
