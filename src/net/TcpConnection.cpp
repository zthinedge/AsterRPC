#include "minirpc/net/TcpConnection.h"

#include "minirpc/net/EventLoop.h"

#include <cerrno>
#include <sys/socket.h>
#include <utility>

namespace minirpc::net{

TcpConnection::TcpConnection(EventLoop*loop,Socket socket)
    :loop_(loop),
     socket_(std::move(socket)),
     channel_(loop,socket_.GetFd()),
     lifetime_token_(std::make_shared<const int>(0)),
     close_after_write_(false),
     closed_(false){

    channel_.SetReadCallback([this](){
        HandleRead();
    });

    channel_.SetWriteCallback([this](){
        HandleWrite();
    });

    channel_.SetCloseCallback([this](){
        HandleClose();
    });
}

TcpConnection::~TcpConnection(){
    if(channel_.IsInEpoll()){
        try{
            channel_.DisableAll();
        }catch(...){
        }
    }
}
//连接对应的channel为可读
void TcpConnection::Start(){
    if(!loop_->IsInLoopThread()){
        std::weak_ptr<TcpConnection> weak=weak_from_this();
        loop_->QueueInLoop([weak](){
            if(auto connection=weak.lock()){
                connection->StartInLoop();
            }
        });
        return;
    }

    StartInLoop();
}

void TcpConnection::StartInLoop(){
    if(closed_){
        return;
    }
    channel_.EnableReading();
}

void TcpConnection::Send(const std::string& data){
    if(data.empty()){
        return;
    }

    if(!loop_->IsInLoopThread()){
        std::weak_ptr<TcpConnection> weak=weak_from_this();
        loop_->QueueInLoop([weak,data](){
            if(auto connection=weak.lock()){
                connection->SendInLoop(data);
            }
        });
        return;
    }

    SendInLoop(data);
}

void TcpConnection::SendInLoop(const std::string& data){
    if(data.empty()||closed_){
        return;
    }

    if(output_buffer_.ReadableBytes()!=0){
        output_buffer_.Append(data);
        channel_.EnableWriting();
        return;
    }

    size_t sent=0;
    //没有积压时直接发送
    while(true){
        ssize_t n=::send(
            socket_.GetFd(),
            data.data(),
            data.size(),
            MSG_NOSIGNAL
        );

        if(n>=0){
            sent=static_cast<size_t>(n);
            break;
        }

        if(errno==EINTR){
            continue;
        }

        if(errno==EAGAIN||errno==EWOULDBLOCK){
            break;
        }

        HandleClose();
        return;
    }

    if(sent<data.size()){
        output_buffer_.Append(
            data.data()+sent,
            data.size()-sent
        );
        channel_.EnableWriting();
    }
}

void TcpConnection::Shutdown(){
    if(!loop_->IsInLoopThread()){
        std::weak_ptr<TcpConnection> weak=weak_from_this();
        loop_->QueueInLoop([weak](){
            if(auto connection=weak.lock()){
                connection->ShutdownInLoop();
            }
        });
        return;
    }

    ShutdownInLoop();
}

void TcpConnection::ShutdownInLoop(){
    if(closed_){
        return;
    }
    if(output_buffer_.ReadableBytes()==0){
        HandleClose();
        return;
    }
    close_after_write_=true;
}

void TcpConnection::Close(){
    if(!loop_->IsInLoopThread()){
        std::weak_ptr<TcpConnection> weak=weak_from_this();
        loop_->QueueInLoop([weak](){
            if(auto connection=weak.lock()){
                connection->CloseInLoop();
            }
        });
        return;
    }

    CloseInLoop();
}

void TcpConnection::CloseInLoop(){
    HandleClose();
}

void TcpConnection::SetMessageCallback(MessageCallback cb){
    message_callback_=std::move(cb);
}

void TcpConnection::SetCloseCallback(CloseCallback cb){
    close_callback_=std::move(cb);
}

int TcpConnection::Fd()const noexcept{
    return socket_.GetFd();
}

EventLoop* TcpConnection::OwnerLoop()const noexcept{
    return loop_;
}

std::weak_ptr<TcpConnection>
TcpConnection::WeakFromThis()noexcept{
    return weak_from_this();
}

std::weak_ptr<const int>
TcpConnection::LifetimeToken()const noexcept{
    return lifetime_token_;
}
//将内核缓冲区数据加到input_buffer
void TcpConnection::HandleRead(){
    char data[4096];
    bool received=false;
    bool should_close=false;

    while(true){
        ssize_t n=::recv(socket_.GetFd(),data,sizeof(data),0);

        if(n>0){
            input_buffer_.Append(data,static_cast<size_t>(n));
            received=true;
            continue;
        }

        if(n==0){
            should_close=true;
            break;
        }

        if(errno==EINTR){
            continue;
        }

        if(errno==EAGAIN||errno==EWOULDBLOCK){
            break;
        }

        should_close=true;
        break;
    }

    if(received&&message_callback_){
        message_callback_(this,&input_buffer_);
    }

    if(should_close){
        HandleClose();
    }
}

void TcpConnection::HandleWrite(){
    while(output_buffer_.ReadableBytes()!=0){
        ssize_t n=::send(
            socket_.GetFd(),
            output_buffer_.Peek(),
            output_buffer_.ReadableBytes(),
            MSG_NOSIGNAL
        );

        if(n>0){
            //偏移
            output_buffer_.Retrieve(static_cast<size_t>(n));
            continue;
        }

        if(n==-1&&errno==EINTR){
            continue;
        }

        if(n==-1&&(errno==EAGAIN||errno==EWOULDBLOCK)){
            return;
        }

        HandleClose();
        return;
    }

    channel_.DisableWriting();
    if(close_after_write_){
        HandleClose();
    }
}

void TcpConnection::HandleClose(){
    if(closed_){
        return;
    }

    closed_=true;
    channel_.DisableAll();

    if(close_callback_){
        close_callback_(this);
    }
}

}
