#include "asterrpc/net/Channel.h"
#include "asterrpc/net/EventLoop.h"
#include <sys/epoll.h>
namespace asterrpc::net
{

Channel::Channel(EventLoop*loop,int fd)
    :loop_(loop),fd_(fd),inepoll_(false),events_(0),revents_(0){}


void Channel::HandleEvent() {
    //fd出错
    if (revents_ & EPOLLERR) {
        if (error_callback_) {
            error_callback_();
        }
    }

    //连接挂断并且没有待读数据
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (close_callback_) {
            close_callback_();
        }
    }

    //可读，紧急数据可读，对方关闭写方向（半关闭）
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (read_callback_) {
            read_callback_();
        }
    }

    //可以发送数据
    if (revents_ & EPOLLOUT) {
        if (write_callback_) {
            write_callback_();
        }
    }
}

int Channel::Fd()const noexcept{
    return fd_;
}

uint32_t Channel::Events()const noexcept{
    return events_;
}
uint32_t Channel::Revents() const noexcept{
    return revents_;
}

//Poller 把epoll实际检测到的事件保存进channel
void Channel::SetRevents(uint32_t revents) noexcept{
    revents_=revents;
}

void Channel::EnableReading(){
    events_|=EPOLLIN;
    Update();
}
void Channel::EnableWriting(){
    events_|=EPOLLOUT;
    Update();
}
void Channel::DisableWriting(){
    events_ &= ~EPOLLOUT;
    Update();
}
void Channel::DisableAll(){
    events_=0;
    Update();
}

void Channel::SetReadCallback(Callback cb){
    read_callback_=std::move(cb);
}
void Channel::SetWriteCallback(Callback cb){
    write_callback_=std::move(cb);
}
void Channel::SetCloseCallback(Callback cb){
    close_callback_=std::move(cb);
}
void Channel::SetErrorCallback(Callback cb){
    error_callback_=std::move(cb);
}

void Channel::Update(){
    loop_->UpdateChannel(this);
}

bool Channel::IsInEpoll()const noexcept{
    return inepoll_;
}

void Channel::SetInEpoll(bool inepoll)noexcept{
    inepoll_=inepoll;
}
}