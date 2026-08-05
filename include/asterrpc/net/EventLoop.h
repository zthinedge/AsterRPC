#pragma once
#include "asterrpc/net/Channel.h"
#include "asterrpc/net/Poller.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace asterrpc::net{

class TimerQueue;

class EventLoop
{


public:
    using Functor=std::function<void()>;
    using TimerId=std::uint64_t;
    using Clock=std::chrono::steady_clock;
    using TimePoint=Clock::time_point;

    EventLoop();
    ~EventLoop();
    void UpdateChannel(Channel*ch);
    void RemoveChannel(Channel*ch);
    void RunInLoop(Functor cb);
    void QueueInLoop(Functor cb);
    TimerId RunAt(TimePoint expiration,Functor cb);
    TimerId RunAfter(std::chrono::microseconds delay,Functor cb);
    void CancelTimer(TimerId timer_id);
    void Loop();
    void Stop()noexcept;
    bool IsInLoopThread()const noexcept;
private:
    void DoPendingFunctors();
    void Wakeup()noexcept;
    void HandleWakeup()noexcept;

    Poller ep_;
    std::atomic_bool stop_;
    Poller::ChannelList channels;

    const std::thread::id thread_id_;
    int wakeup_fd_;
    Channel wakeup_channel_;
    std::unique_ptr<TimerQueue> timer_queue_;

    std::mutex mutex_;
    std::vector<Functor> pending_functors_;
    bool calling_pending_functors_;
    
};


}
