#pragma once

#include "asterrpc/net/Channel.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

namespace asterrpc::net{

class EventLoop;

class TimerQueue{
public:
    using TimerId=std::uint64_t;
    using Clock=std::chrono::steady_clock;
    using TimePoint=Clock::time_point;
    using Callback=std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerQueue(const TimerQueue&)=delete;
    TimerQueue& operator=(const TimerQueue&)=delete;

    TimerId AddTimer(TimePoint expiration,Callback callback);
    void Cancel(TimerId timer_id);

private:
    using TimerKey=std::pair<TimePoint,TimerId>;

    void AddTimerInLoop(
        TimePoint expiration,
        TimerId timer_id,
        Callback callback
    );
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ResetTimerFd();

    EventLoop* loop_;
    int timer_fd_;
    Channel timer_channel_;
    std::atomic_uint64_t next_timer_id_;
    std::map<TimerKey,Callback> timers_;
    std::unordered_map<TimerId,TimePoint> expirations_;
};

}
