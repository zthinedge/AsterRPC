#include "asterrpc/net/Poller.h"
#include "asterrpc/net/Channel.h"
#include <unistd.h>
#include <stdexcept>
#include <cerrno>
namespace asterrpc::net{

Poller::Poller(EventLoop*loop)
    :loop_(loop),epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),events_(16){
    
    if(epoll_fd_==-1){
        throw std::runtime_error("epoll_create1 failed");
    }
}

Poller::~Poller(){
    ::close(epoll_fd_);
}

void Poller::Poll(int timeout_ms,ChannelList*chs){
    chs->clear();
    int nready=epoll_wait(epoll_fd_,events_.data(),events_.size(),timeout_ms);
    if(nready==-1){
        //被信号中断
        if(errno==EINTR){
            return;
        }
        throw std::runtime_error("epoll_wait failed");
    }

    for(int i=0;i<nready;i++){
        Channel*ch=static_cast<Channel*>(events_[i].data.ptr);
        ch->SetRevents(events_[i].events);
        chs->push_back(ch);
    }
    //减少调用epoll_wait()次数
    if(nready==static_cast<int>(events_.size())){
        events_.resize(events_.size()*2);
    }
}
void Poller::UpdateChannel(Channel*ch){
    epoll_event ev{};
    ev.events=ch->Events();
    ev.data.ptr=ch;
    if(ch->IsInEpoll()){
        if(ch->Events()==0){
            if(::epoll_ctl(
                epoll_fd_,
                EPOLL_CTL_DEL,
                ch->Fd(),
                nullptr
            )==-1){
                throw std::runtime_error("epoll_ctl del failed");
            }

            ch->SetInEpoll(false);
        }else{
            if(::epoll_ctl(
                epoll_fd_,
                EPOLL_CTL_MOD,
                ch->Fd(),
                &ev
            )==-1){
                throw std::runtime_error("epoll_ctl mod failed");
            }
        }
    }else{
        if(ch->Events()==0){
            return;
        }

        if(::epoll_ctl(
            epoll_fd_,
            EPOLL_CTL_ADD,
            ch->Fd(),
            &ev
        )==-1){
            throw std::runtime_error("epoll_ctl add failed");
        }

        ch->SetInEpoll(true);
    }
}
void Poller::RemoveChannel(Channel*ch){
    if(!ch->IsInEpoll()){
        return;
    }

    if(::epoll_ctl(
        epoll_fd_,
        EPOLL_CTL_DEL,
        ch->Fd(),
        nullptr
    )==-1){
        throw std::runtime_error("epoll_ctl del failed");
    }

    ch->SetInEpoll(false);
}

}
