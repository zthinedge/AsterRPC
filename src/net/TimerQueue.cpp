#include "asterrpc/net/TimerQueue.h"

#include "asterrpc/net/EventLoop.h"

#include <cerrno>
#include <stdexcept>
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>

namespace asterrpc::net{
namespace{

int CreateTimerFd(){
    int fd=::timerfd_create(
        CLOCK_MONOTONIC,
        TFD_NONBLOCK|TFD_CLOEXEC
    );

    if(fd==-1){
        throw std::runtime_error("timerfd_create failed");
    }

    return fd;
}

timespec ToTimespec(TimerQueue::TimePoint expiration){
    auto delay=expiration-TimerQueue::Clock::now();
    auto nanoseconds=
        std::chrono::duration_cast<std::chrono::nanoseconds>(delay);

    if(nanoseconds.count()<=0){
        nanoseconds=std::chrono::nanoseconds(1);
    }

    timespec value{};
    value.tv_sec=static_cast<time_t>(
        nanoseconds.count()/1000000000
    );
    value.tv_nsec=static_cast<long>(
        nanoseconds.count()%1000000000
    );
    return value;
}

}

TimerQueue::TimerQueue(EventLoop* loop)
    :loop_(loop),
     timer_fd_(CreateTimerFd()),
     timer_channel_(loop,timer_fd_),
     next_timer_id_(1){
    try{
        timer_channel_.SetReadCallback([this](){
            HandleRead();
        });
        timer_channel_.EnableReading();
    }catch(...){
        ::close(timer_fd_);
        throw;
    }
}

TimerQueue::~TimerQueue(){
    if(timer_channel_.IsInEpoll()){
        try{
            timer_channel_.DisableAll();
        }catch(...){
        }
    }

    ::close(timer_fd_);
}

TimerQueue::TimerId TimerQueue::AddTimer(
    TimePoint expiration,
    Callback callback
){
    if(!callback){
        throw std::invalid_argument("timer callback is empty");
    }

    TimerId timer_id=next_timer_id_.fetch_add(1);
    while(timer_id==0){
        timer_id=next_timer_id_.fetch_add(1);
    }

    loop_->RunInLoop(
        [this,expiration,timer_id,callback=std::move(callback)]()mutable{
            AddTimerInLoop(
                expiration,
                timer_id,
                std::move(callback)
            );
        }
    );

    return timer_id;
}

void TimerQueue::Cancel(TimerId timer_id){
    if(timer_id==0){
        return;
    }

    loop_->RunInLoop([this,timer_id](){
        CancelInLoop(timer_id);
    });
}

void TimerQueue::AddTimerInLoop(
    TimePoint expiration,
    TimerId timer_id,
    Callback callback
){
    TimerKey key{expiration,timer_id};
    bool reset=timers_.empty()||key<timers_.begin()->first;

    timers_.emplace(key,std::move(callback));
    expirations_.emplace(timer_id,expiration);

    if(reset){
        ResetTimerFd();
    }
}

void TimerQueue::CancelInLoop(TimerId timer_id){
    auto expiration=expirations_.find(timer_id);
    if(expiration==expirations_.end()){
        return;
    }

    TimerKey key{expiration->second,timer_id};
    bool reset=!timers_.empty()&&key==timers_.begin()->first;

    timers_.erase(key);
    expirations_.erase(expiration);

    if(reset){
        ResetTimerFd();
    }
}

void TimerQueue::HandleRead(){
    std::uint64_t expirations=0;

    while(::read(timer_fd_,&expirations,sizeof(expirations))==-1){
        if(errno==EINTR){
            continue;
        }
        break;
    }

    TimePoint now=Clock::now();
    std::vector<Callback> callbacks;

    while(!timers_.empty()&&timers_.begin()->first.first<=now){
        auto timer=timers_.begin();
        expirations_.erase(timer->first.second);
        callbacks.push_back(std::move(timer->second));
        timers_.erase(timer);
    }

    ResetTimerFd();

    for(auto& callback:callbacks){
        callback();
    }
}

void TimerQueue::ResetTimerFd(){
    itimerspec value{};

    if(!timers_.empty()){
        value.it_value=ToTimespec(timers_.begin()->first.first);
    }

    if(::timerfd_settime(timer_fd_,0,&value,nullptr)==-1){
        throw std::runtime_error("timerfd_settime failed");
    }
}

}
