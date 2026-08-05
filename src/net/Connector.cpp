#include "asterrpc/net/Connector.h"

#include <cerrno>
#include <sys/socket.h>
#include <utility>

namespace asterrpc::net{

Connector::Connector(EventLoop*loop,const InetAddress& server_addr)
    :server_addr_(server_addr),
     socket_(),
     channel_(loop,socket_.GetFd()),
     state_(State::Disconnected){
    socket_.SetNonBlocking();

    channel_.SetWriteCallback([this](){
        HandleWrite();
    });

    channel_.SetErrorCallback([this](){
        HandleError();
    });

    channel_.SetCloseCallback([this](){
        HandleError();
    });
}

Connector::~Connector(){
    if(channel_.IsInEpoll()){
        try{
            channel_.DisableAll();
        }catch(...){
        }
    }
}

void Connector::SetNewConnectionCallback(NewConnectionCallback cb){
    new_connection_callback_=std::move(cb);
}

void Connector::SetErrorCallback(ErrorCallback cb){
    error_callback_=std::move(cb);
}

void Connector::Connect(){
    if(state_!=State::Disconnected){
        return;
    }

    int result=::connect(
        socket_.GetFd(),
        server_addr_.GetSockAddr(),
        server_addr_.GetSockLen()
    );

    if(result==0){
        state_=State::Connected;
        NotifyConnected();
        return;
    }

    if(errno==EINPROGRESS||errno==EINTR||errno==EALREADY){
        state_=State::Connecting;
        channel_.EnableWriting();
        return;
    }

    if(errno==EISCONN){
        state_=State::Connected;
        NotifyConnected();
        return;
    }

    int error=errno;
    state_=State::Failed;
    NotifyError(error);
}

void Connector::HandleWrite(){
    if(state_!=State::Connecting){
        return;
    }

    int error=GetSocketError();
    channel_.DisableAll();

    if(error!=0){
        state_=State::Failed;
        NotifyError(error);
        return;
    }

    state_=State::Connected;
    NotifyConnected();
}

void Connector::HandleError(){
    if(state_!=State::Connecting){
        return;
    }

    int error=GetSocketError();
    channel_.DisableAll();
    state_=State::Failed;

    if(error==0){
        error=ECONNABORTED;
    }

    NotifyError(error);
}

int Connector::GetSocketError()const noexcept{
    int error=0;
    socklen_t len=sizeof(error);

    if(::getsockopt(
        socket_.GetFd(),
        SOL_SOCKET,
        SO_ERROR,
        &error,
        &len
    )==-1){
        return errno;
    }

    return error;
}

void Connector::NotifyConnected(){
    if(new_connection_callback_){
        new_connection_callback_(std::move(socket_));
    }
}

void Connector::NotifyError(int error){
    if(error_callback_){
        error_callback_(error);
    }
}

}
