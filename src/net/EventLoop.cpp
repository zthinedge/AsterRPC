#include "minirpc/net/EventLoop.h"
#include "minirpc/net/TimerQueue.h"

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <memory>

namespace minirpc::net{

namespace{

int CreateEventFd(){
    int fd=::eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);
    if(fd==-1){
        throw std::runtime_error("eventfd failed");
    }
    return fd;
}

}

EventLoop::EventLoop()
    :ep_(this),
     stop_(false),
     thread_id_(std::this_thread::get_id()),
     wakeup_fd_(CreateEventFd()),
     wakeup_channel_(this,wakeup_fd_),
     calling_pending_functors_(false){
    try{
        wakeup_channel_.SetReadCallback([this](){
            HandleWakeup();
        });
        wakeup_channel_.EnableReading();
        timer_queue_=std::make_unique<TimerQueue>(this);
    }catch(...){
        ::close(wakeup_fd_);
        throw;
    }
}

EventLoop::~EventLoop(){
    timer_queue_.reset();

    if(wakeup_channel_.IsInEpoll()){
        try{
            wakeup_channel_.DisableAll();
        }catch(...){
        }
    }
    ::close(wakeup_fd_);
}

void EventLoop::UpdateChannel(Channel*ch){
    ep_.UpdateChannel(ch);
}
void EventLoop::RemoveChannel(Channel*ch){
    ep_.RemoveChannel(ch);
}

void EventLoop::RunInLoop(Functor cb){
    if(IsInLoopThread()){
        cb();
        return;
    }

    QueueInLoop(std::move(cb));
}

void EventLoop::QueueInLoop(Functor cb){
    {
        std::lock_guard<std::mutex>lock(mutex_);
        pending_functors_.push_back(std::move(cb));
    }

    if(!IsInLoopThread()||calling_pending_functors_){
        Wakeup();
    }
}

EventLoop::TimerId EventLoop::RunAt(
    TimePoint expiration,
    Functor cb
){
    return timer_queue_->AddTimer(expiration,std::move(cb));
}

EventLoop::TimerId EventLoop::RunAfter(
    std::chrono::microseconds delay,
    Functor cb
){
    return RunAt(Clock::now()+delay,std::move(cb));
}

void EventLoop::CancelTimer(TimerId timer_id){
    timer_queue_->Cancel(timer_id);
}

void EventLoop::Loop(){
    while(!stop_.load()){   
        ep_.Poll(-1,&channels);

        for(auto ch:channels){
            ch->HandleEvent();
        }

        channels.clear();
        DoPendingFunctors();
    }
}

void EventLoop::DoPendingFunctors(){
    calling_pending_functors_=true;

    std::vector<Functor> functors;
    {
        std::lock_guard<std::mutex>lock(mutex_);
        functors.swap(pending_functors_);
    }

    //防止在执行任务时调用QueueInLoop后，任务一直等到下次IO事件
    for(auto& functor:functors){
        functor();
    }

    calling_pending_functors_=false;
}

void EventLoop::Stop()noexcept{
    stop_.store(true);
    Wakeup();
}

bool EventLoop::IsInLoopThread()const noexcept{
    return thread_id_==std::this_thread::get_id();
}

void EventLoop::Wakeup()noexcept{
    std::uint64_t value=1;

    while(::write(wakeup_fd_,&value,sizeof(value))==-1){
        if(errno==EINTR){
            continue;
        }
        break;
    }
}

void EventLoop::HandleWakeup()noexcept{
    std::uint64_t value=0;

    while(::read(wakeup_fd_,&value,sizeof(value))==-1){
        if(errno==EINTR){
            continue;
        }
        break;
    }
}
}
