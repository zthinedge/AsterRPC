#include "asterrpc/net/Acceptor.h"
#include "asterrpc/net/InetAddress.h"

#include <utility>

namespace asterrpc::net{

Acceptor::Acceptor(EventLoop*loop,const InetAddress& addr)
    :listen_socket_(),
     accept_channel_(loop,listen_socket_.GetFd()){
    listen_socket_.SetReuseAddr(true);
    listen_socket_.SetNonBlocking();
    listen_socket_.Bind(addr);

    accept_channel_.SetReadCallback([this](){
        HandleRead();
    });
}

void Acceptor::SetNewConnectionCallback(NewConnectionCallback cb){
    new_connection_callback_=std::move(cb);
}

void Acceptor::Listen(){
    listen_socket_.Listen();
    accept_channel_.EnableReading();
}

void Acceptor::HandleRead(){
    InetAddress peer_addr("0.0.0.0",0);
    Socket client_socket=listen_socket_.Accept(&peer_addr);
    client_socket.SetNonBlocking();

    if(new_connection_callback_){
        new_connection_callback_(std::move(client_socket),peer_addr);
    }
}

}
