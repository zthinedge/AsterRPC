#include "asterrpc/cluster/ChannelManager.h"
#include "asterrpc/cluster/ConnectionPool.h"
#include "asterrpc/cluster/Endpoint.h"
#include "asterrpc/cluster/RetryPolicy.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/protocol/RpcMessage.h"
#include "asterrpc/rpc/CallOptions.h"
#include "asterrpc/rpc/RpcServer.h"
#include "asterrpc/trace/TraceContext.h"

#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace asterrpc;

namespace{

std::uint16_t FindFreePort(){
    int fd=::socket(AF_INET,SOCK_STREAM,0);
    assert(fd!=-1);

    sockaddr_in address{};
    address.sin_family=AF_INET;
    address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    address.sin_port=0;

    assert(::bind(
        fd,
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address)
    )==0);

    socklen_t size=sizeof(address);
    assert(::getsockname(
        fd,
        reinterpret_cast<sockaddr*>(&address),
        &size
    )==0);

    std::uint16_t port=ntohs(address.sin_port);
    ::close(fd);
    return port;
}

class LoopThread{
public:
    LoopThread(){
        std::promise<net::EventLoop*> ready;
        auto future=ready.get_future();

        thread_=std::thread([ready=std::move(ready)]()mutable{
            net::EventLoop loop;
            ready.set_value(&loop);
            loop.Loop();
        });
        loop_=future.get();
    }

    ~LoopThread(){
        Stop();
    }

    LoopThread(const LoopThread&)=delete;
    LoopThread& operator=(const LoopThread&)=delete;

    net::EventLoop* Loop()const noexcept{
        return loop_;
    }

    void Stop(){
        if(thread_.joinable()){
            loop_->Stop();
            thread_.join();
        }
    }

private:
    net::EventLoop* loop_=nullptr;
    std::thread thread_;
};

class EchoServer{
public:
    explicit EchoServer(std::uint16_t port):port_(port){
        std::promise<net::EventLoop*> ready;
        auto future=ready.get_future();

        thread_=std::thread(
            [this,ready=std::move(ready)]()mutable{
                net::EventLoop loop;
                net::InetAddress address("127.0.0.1",port_);
                rpc::RpcServer server(&loop,address);
                server.RegisterMethod(
                    "EchoService",
                    "Echo",
                    [this](const std::string& payload){
                        calls_.fetch_add(1);
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(5)
                        );
                        return payload;
                    }
                );
                server.Start();
                ready.set_value(&loop);
                loop.Loop();
            }
        );
        loop_=future.get();
    }

    ~EchoServer(){
        Stop();
    }

    EchoServer(const EchoServer&)=delete;
    EchoServer& operator=(const EchoServer&)=delete;

    void Stop(){
        if(thread_.joinable()){
            loop_->Stop();
            thread_.join();
        }
    }

    int Calls()const noexcept{
        return calls_.load();
    }

private:
    std::uint16_t port_;
    std::atomic_int calls_{0};
    net::EventLoop* loop_=nullptr;
    std::thread thread_;
};

template<class Predicate>
bool WaitUntil(Predicate predicate,std::chrono::milliseconds timeout){
    auto deadline=std::chrono::steady_clock::now()+timeout;
    while(std::chrono::steady_clock::now()<deadline){
        if(predicate()){
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

protocol::RpcMessage GetResponse(
    cluster::ConnectionPool::ResponseFuture future
){
    assert(future.wait_for(std::chrono::seconds(3))==
           std::future_status::ready);
    return future.get();
}

rpc::CallOptions ShortTimeout(){
    rpc::CallOptions options;
    options.idempotent=true;
    options.timeout=std::chrono::seconds(1);
    return options;
}

void TestEndpointAndRetryPolicy(){
    cluster::Endpoint first("127.0.0.1",9000);
    cluster::Endpoint same("127.0.0.1",9000);
    cluster::Endpoint other("127.0.0.1",9001);

    assert(first==same);
    assert(first!=other);
    assert(first.ToString()=="127.0.0.1:9000");
    assert(cluster::EndpointHash{}(first)==
           cluster::EndpointHash{}(same));

    cluster::RetryPolicy retry(
        3,std::chrono::milliseconds(10),2.0,
        std::chrono::milliseconds(100),0.0
    );
    assert(retry.ShouldRetry(
        protocol::StatusCode::ConnectionFailed,1
    ));
    assert(!retry.ShouldRetry(protocol::StatusCode::Timeout,1));
    assert(!retry.ShouldRetry(
        protocol::StatusCode::ConnectionFailed,3
    ));
    assert(retry.BackoffForRetry(1)==
           std::chrono::milliseconds(10));
    assert(retry.BackoffForRetry(2)==
           std::chrono::milliseconds(20));

    cluster::RetryPolicy jittered(
        3,std::chrono::milliseconds(100),2.0,
        std::chrono::seconds(1),0.2
    );
    auto first_delay=jittered.BackoffForRetry(1);
    bool varied=false;
    for(int sample=0;sample<20;++sample){
        auto delay=jittered.BackoffForRetry(1);
        assert(delay>=std::chrono::milliseconds(80));
        assert(delay<=std::chrono::milliseconds(120));
        varied=varied||delay!=first_delay;
    }
    assert(varied);
}

void TestChannelManagerReusesEndpoint(){
    LoopThread loop_thread;
    cluster::ChannelManager manager(loop_thread.Loop());
    cluster::Endpoint endpoint("127.0.0.1",19001);
    cluster::ConnectionPoolOptions options;
    options.max_connections=2;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto first=manager.GetOrCreate(endpoint,options);
    auto second=manager.GetOrCreate(endpoint,options);
    assert(first==second);
    assert(manager.Size()==1);
    assert(manager.Find(endpoint)==first);
    auto snapshot=manager.Snapshot();
    assert(snapshot.size()==1);
    assert(snapshot.front()==first);

    cluster::ConnectionPoolOptions different=options;
    different.max_connections=3;
    bool rejected=false;
    try{
        manager.GetOrCreate(endpoint,different);
    }catch(const std::logic_error&){
        rejected=true;
    }
    assert(rejected);
    assert(manager.Remove(endpoint));
    assert(manager.Size()==0);
    assert(manager.Snapshot().empty());
}

void TestConnectionFailureIsExplicit(){
    std::uint16_t port=FindFreePort();
    LoopThread loop_thread;
    cluster::ConnectionPoolOptions options;
    options.max_connections=2;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        loop_thread.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );

    rpc::CallOptions call_options;
    call_options.timeout=std::chrono::seconds(1);
    cluster::RetryPolicy retry(
        3,std::chrono::milliseconds(1),2.0,
        std::chrono::milliseconds(10),0.0
    );
    protocol::RpcMessage response=GetResponse(pool->FutureCall(
        "EchoService","Create","unreachable",call_options,retry
    ));

    assert(response.meta.status_code==
           protocol::StatusCode::ConnectionFailed);
    assert(response.meta.error_text.find("127.0.0.1:")!=
           std::string::npos);
    assert(response.meta.error_text.find("failed")!=
           std::string::npos);
    assert(pool->GetStats().retries==0);
    assert(WaitUntil(
        [&pool](){
            return pool->GetStats().connections==0;
        },
        std::chrono::seconds(1)
    ));

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.successful_requests==0);
    assert(metrics.failed_requests==1);
    assert(metrics.timeout_requests==0);
    assert(metrics.retries==0);
    assert(metrics.inflight_requests==0);
    assert(metrics.active_connections==0);
}

void TestRetryCanRecoverOnANewConnection(){
    std::uint16_t port=FindFreePort();
    LoopThread loop_thread;
    cluster::ConnectionPoolOptions options;
    options.max_connections=1;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        loop_thread.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );

    std::unique_ptr<EchoServer> server;
    std::thread delayed_server([&server,port](){
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        server=std::make_unique<EchoServer>(port);
    });

    cluster::RetryPolicy retry(
        3,
        std::chrono::milliseconds(100),
        1.0,
        std::chrono::milliseconds(100),
        0.0
    );
    protocol::RpcMessage response=GetResponse(pool->FutureCall(
        "EchoService","Echo","retry-success",ShortTimeout(),retry
    ));

    delayed_server.join();
    assert(response.meta.status_code==protocol::StatusCode::Ok);
    assert(response.payload=="retry-success");
    assert(server->Calls()==1);
    assert(pool->GetStats().retries==1);

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.successful_requests==1);
    assert(metrics.failed_requests==0);
    assert(metrics.retries==1);
    assert(metrics.inflight_requests==0);
    assert(metrics.active_connections==1);
    assert(metrics.latency_samples==1);
}

void TestBusinessErrorsAreNotRetried(){
    std::uint16_t port=FindFreePort();
    EchoServer server(port);
    LoopThread loop_thread;
    cluster::ConnectionPoolOptions options;
    options.max_connections=1;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        loop_thread.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );
    cluster::RetryPolicy retry(
        3,std::chrono::milliseconds(1),2.0,
        std::chrono::milliseconds(10),0.0
    );

    protocol::RpcMessage response=GetResponse(pool->FutureCall(
        "EchoService","MissingMethod","",ShortTimeout(),retry
    ));
    assert(response.meta.status_code==
           protocol::StatusCode::MethodNotFound);
    assert(pool->GetStats().retries==0);

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.successful_requests==0);
    assert(metrics.failed_requests==1);
    assert(metrics.retries==0);
}

void TestTraceContextCrossesPoolThread(){
    std::uint16_t port=FindFreePort();
    EchoServer server(port);
    LoopThread client_loop;
    cluster::ConnectionPoolOptions options;
    options.max_connections=1;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        client_loop.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );

    std::uint64_t deadline=static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    )+1000000;
    trace::TraceContext parent=trace::CreateRootTrace(deadline);

    protocol::RpcMessage response;
    {
        trace::TraceScope scope(parent);
        response=GetResponse(pool->FutureCall(
            "EchoService",
            "Echo",
            "traced",
            {}
        ));
    }

    assert(response.meta.status_code==protocol::StatusCode::Ok);
    assert(response.meta.trace_id==parent.trace_id);
    assert(response.meta.deadline_us==deadline);
    assert(!response.meta.span_id.empty());
    assert(!response.meta.parent_span_id.empty());
    assert(response.meta.parent_span_id!=parent.span_id);

    auto method_metrics=pool->GetMethodMetrics(
        "EchoService",
        "Echo"
    );
    assert(method_metrics.total_requests==1);
    assert(method_metrics.successful_requests==1);
    assert(pool->GetAllMethodMetrics().size()==1);
}

void TestRetryDoesNotExceedDeadline(){
    std::uint16_t port=FindFreePort();
    LoopThread loop_thread;
    cluster::ConnectionPoolOptions options;
    options.max_connections=1;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        loop_thread.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );
    rpc::CallOptions call_options;
    call_options.idempotent=true;
    call_options.timeout=std::chrono::milliseconds(30);
    cluster::RetryPolicy retry(
        3,std::chrono::milliseconds(100),2.0,
        std::chrono::milliseconds(200),0.0
    );

    protocol::RpcMessage response=GetResponse(pool->FutureCall(
        "EchoService","Echo","",call_options,retry
    ));
    assert(response.meta.status_code==protocol::StatusCode::Timeout);
    assert(pool->GetStats().retries==0);

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==1);
    assert(metrics.failed_requests==1);
    assert(metrics.timeout_requests==1);
    assert(metrics.retries==0);
}

void TestReuseLimitDistributionAndInvalidRemoval(){
    std::uint16_t port=FindFreePort();
    EchoServer server(port);
    LoopThread client_loop;
    cluster::ConnectionPoolOptions options;
    options.max_connections=3;
    options.idle_timeout=std::chrono::milliseconds::zero();
    options.reap_interval=std::chrono::milliseconds(10);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        client_loop.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );

    std::vector<cluster::ConnectionPool::ResponseFuture> futures;
    for(int index=0;index<12;++index){
        futures.push_back(pool->FutureCall(
            "EchoService",
            "Echo",
            "payload-"+std::to_string(index),
            ShortTimeout()
        ));
    }

    for(int index=0;index<12;++index){
        protocol::RpcMessage response=GetResponse(
            std::move(futures[index])
        );
        assert(response.meta.status_code==protocol::StatusCode::Ok);
        assert(response.payload=="payload-"+std::to_string(index));
    }

    cluster::ConnectionPoolStats stats=pool->GetStats();
    assert(stats.connections==options.max_connections);
    assert(stats.connected==options.max_connections);
    assert(stats.in_flight==0);

    protocol::RpcMessage reused=GetResponse(pool->FutureCall(
        "EchoService","Echo","reused",ShortTimeout()
    ));
    assert(reused.meta.status_code==protocol::StatusCode::Ok);
    assert(reused.payload=="reused");
    assert(pool->GetStats().connections==options.max_connections);

    server.Stop();
    assert(WaitUntil(
        [&pool](){
            return pool->GetStats().connections==0;
        },
        std::chrono::seconds(2)
    ));

    protocol::RpcMessage failed=GetResponse(pool->FutureCall(
        "EchoService","Echo","after-close",ShortTimeout()
    ));
    assert(failed.meta.status_code==
           protocol::StatusCode::ConnectionFailed);
    assert(server.Calls()==13);

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==14);
    assert(metrics.successful_requests==13);
    assert(metrics.failed_requests==1);
    assert(metrics.inflight_requests==0);
    assert(metrics.active_connections==0);
}

void TestIdleConnectionReaping(){
    std::uint16_t port=FindFreePort();
    EchoServer server(port);
    LoopThread client_loop;
    cluster::ConnectionPoolOptions options;
    options.max_connections=2;
    options.idle_timeout=std::chrono::milliseconds(50);
    options.reap_interval=std::chrono::milliseconds(20);

    auto pool=std::make_shared<cluster::ConnectionPool>(
        client_loop.Loop(),
        cluster::Endpoint("127.0.0.1",port),
        options
    );

    protocol::RpcMessage first=GetResponse(pool->FutureCall(
        "EchoService","Echo","first",ShortTimeout()
    ));
    assert(first.meta.status_code==protocol::StatusCode::Ok);
    assert(pool->GetStats().connections==1);

    protocol::RpcMessage second=GetResponse(pool->FutureCall(
        "EchoService","Echo","second",ShortTimeout()
    ));
    assert(second.meta.status_code==protocol::StatusCode::Ok);
    assert(pool->GetStats().connections==1);

    assert(WaitUntil(
        [&pool](){
            return pool->GetStats().connections==0;
        },
        std::chrono::seconds(1)
    ));
    assert(pool->GetMetrics().active_connections==0);

    protocol::RpcMessage recreated=GetResponse(pool->FutureCall(
        "EchoService","Echo","recreated",ShortTimeout()
    ));
    assert(recreated.meta.status_code==protocol::StatusCode::Ok);
    assert(recreated.payload=="recreated");
    assert(pool->GetStats().connections==1);

    auto metrics=pool->GetMetrics();
    assert(metrics.total_requests==3);
    assert(metrics.successful_requests==3);
    assert(metrics.failed_requests==0);
    assert(metrics.active_connections==1);
}

}

int main(){
    TestEndpointAndRetryPolicy();
    TestChannelManagerReusesEndpoint();
    TestConnectionFailureIsExplicit();
    TestRetryCanRecoverOnANewConnection();
    TestBusinessErrorsAreNotRetried();
    TestTraceContextCrossesPoolThread();
    TestRetryDoesNotExceedDeadline();
    TestReuseLimitDistributionAndInvalidRemoval();
    TestIdleConnectionReaping();
    return 0;
}
