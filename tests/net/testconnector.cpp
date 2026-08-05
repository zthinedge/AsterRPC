#include "asterrpc/net/Connector.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"

#include <cassert>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace asterrpc::net;

int main(){
    int listen_fd=::socket(AF_INET,SOCK_STREAM,0);
    assert(listen_fd!=-1);

    sockaddr_in listen_addr{};
    listen_addr.sin_family=AF_INET;
    listen_addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    listen_addr.sin_port=0;

    assert(::bind(
        listen_fd,
        reinterpret_cast<sockaddr*>(&listen_addr),
        sizeof(listen_addr)
    )==0);
    assert(::listen(listen_fd,1)==0);

    socklen_t len=sizeof(listen_addr);
    assert(::getsockname(
        listen_fd,
        reinterpret_cast<sockaddr*>(&listen_addr),
        &len
    )==0);

    EventLoop loop;
    InetAddress server_addr(listen_addr);
    Connector connector(&loop,server_addr);

    bool connected=false;
    int error=0;

    connector.SetNewConnectionCallback([&](Socket socket){
        connected=socket.GetFd()!=-1;
        loop.Stop();
    });

    connector.SetErrorCallback([&](int value){
        error=value;
        loop.Stop();
    });

    connector.Connect();
    loop.Loop();

    assert(connected);
    assert(error==0);

    ::close(listen_fd);
    return 0;
}
