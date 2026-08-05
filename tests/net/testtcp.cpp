#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/net/TcpClient.h"
#include "asterrpc/net/TcpServer.h"

#include <cassert>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

using namespace asterrpc::net;

namespace{

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

}

int main(){
    EventLoop loop;
    InetAddress server_addr("127.0.0.1",FindFreePort());

    TcpServer server(&loop,server_addr);
    server.SetMessageCallback(
        [](TcpConnection* connection,Buffer* buffer){
            connection->Send(buffer->RetrieveAllAsString());
        }
    );
    server.Start();

    TcpClient client(&loop,server_addr);

    bool received=false;
    int error=0;

    client.SetConnectionCallback([&client](TcpConnection*){
        client.Send("hello AsterRPC");
    });

    client.SetMessageCallback(
        [&loop,&received](TcpConnection*,Buffer* buffer){
            std::string message=buffer->RetrieveAllAsString();
            received=message=="hello AsterRPC";
            loop.Stop();
        }
    );

    client.SetErrorCallback([&loop,&error](int value){
        error=value;
        loop.Stop();
    });

    client.Connect();
    loop.Loop();

    assert(received);
    assert(error==0);
    return 0;
}
