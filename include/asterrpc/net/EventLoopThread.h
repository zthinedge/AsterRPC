#pragma once

#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>

namespace asterrpc::net{

class EventLoop;

class EventLoopThread{
public:
    EventLoopThread()=default;
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&)=delete;
    EventLoopThread& operator=(const EventLoopThread&)=delete;
    EventLoopThread(EventLoopThread&&)=delete;
    EventLoopThread& operator=(EventLoopThread&&)=delete;

    EventLoop* StartLoop();
    void Stop()noexcept;

private:
    void ThreadMain()noexcept;

    std::mutex mutex_;
    std::condition_variable ready_;
    EventLoop* loop_=nullptr;
    std::exception_ptr startup_error_;
    bool started_=false;
    std::thread thread_;
};

}
