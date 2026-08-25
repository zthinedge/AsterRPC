#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/rpc/CallOptions.h"
#include "asterrpc/rpc/RpcClient.h"
#include "asterrpc/rpc/RpcServer.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace asterrpc;

namespace{

std::uint16_t FindFreePort(){
    int fd=::socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0);
    assert(fd!=-1);

    sockaddr_in address{};
    address.sin_family=AF_INET;
    address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    address.sin_port=0;
    assert(::bind(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address))==0);

    socklen_t length=sizeof(address);
    assert(::getsockname(
        fd,
        reinterpret_cast<sockaddr*>(&address),
        &length
    )==0);
    std::uint16_t port=ntohs(address.sin_port);
    ::close(fd);
    return port;
}

}

int main(){
    const std::uint16_t port=FindFreePort();
    std::promise<net::EventLoop*> server_ready;
    std::thread server_thread([port,&server_ready](){
        net::EventLoop loop;
        rpc::RpcServerOptions options;
        options.business_threads=2;
        rpc::RpcServer server(
            &loop,
            net::InetAddress("127.0.0.1",port),
            options
        );
        server.RegisterMethod(
            "TestService",
            "Echo",
            [](const std::string& payload){
                return payload;
            }
        );
        server.RegisterMethod(
            "TestService",
            "Slow",
            [](const std::string& payload){
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return payload;
            }
        );
        server.Start();
        server_ready.set_value(&loop);
        loop.Loop();
    });

    net::EventLoop* server_loop=server_ready.get_future().get();
    std::promise<rpc::RpcClient*> client_ready;
    std::promise<net::EventLoop*> client_loop_ready;
    std::thread client_thread([port,&client_ready,&client_loop_ready](){
        net::EventLoop loop;
        rpc::RpcClient client(
            &loop,
            net::InetAddress("127.0.0.1",port)
        );
        client.SetConnectionCallback([&client,&client_ready](){
            client_ready.set_value(&client);
        });
        client_loop_ready.set_value(&loop);
        client.Connect();
        loop.Loop();
    });

    rpc::RpcClient* client=client_ready.get_future().get();
    net::EventLoop* client_loop=client_loop_ready.get_future().get();

    constexpr std::size_t call_count=32;
    std::vector<rpc::RpcClient::ResponseFuture> futures;
    futures.reserve(call_count);
    for(std::size_t index=0;index<call_count;++index){
        futures.push_back(client->FutureCall(
            "TestService",
            "Echo",
            std::to_string(index)
        ));
    }
    for(std::size_t index=0;index<call_count;++index){
        protocol::RpcMessage response=futures[index].get();
        assert(response.meta.status_code==protocol::StatusCode::Ok);
        assert(response.payload==std::to_string(index));
    }

    std::promise<protocol::RpcMessage> callback_result;
    auto callback_future=callback_result.get_future();
    client->AsyncCall(
        "TestService",
        "Echo",
        "callback",
        [&callback_result](protocol::RpcMessage response){
            callback_result.set_value(std::move(response));
        }
    );
    assert(callback_future.get().payload=="callback");

    rpc::CallOptions timeout;
    timeout.timeout=std::chrono::milliseconds(10);
    protocol::RpcMessage timed_out=client->FutureCall(
        "TestService",
        "Slow",
        "late",
        timeout
    ).get();
    assert(timed_out.meta.status_code==protocol::StatusCode::Timeout);

    client_loop->Stop();
    client_thread.join();
    server_loop->Stop();
    server_thread.join();
    return 0;
}
