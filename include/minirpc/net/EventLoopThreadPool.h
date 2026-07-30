#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

namespace minirpc::net{

class EventLoop;
class EventLoopThread;

enum class IoLoopLoadBalance{
    RoundRobin,
    LeastConnections
};

struct EventLoopThreadPoolOptions{
    std::size_t thread_count=0;
    IoLoopLoadBalance load_balance=IoLoopLoadBalance::RoundRobin;
};

class EventLoopThreadPool{
public:
    static constexpr std::size_t kBaseLoopIndex=
        std::numeric_limits<std::size_t>::max();

    struct Selection{
        EventLoop* loop=nullptr;
        std::size_t worker_index=kBaseLoopIndex;
    };

    EventLoopThreadPool(
        EventLoop* base_loop,
        EventLoopThreadPoolOptions options={}
    );
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&)=delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&)=delete;

    void Start();
    void Stop()noexcept;

    Selection AcquireLoop();
    void ReleaseLoop(std::size_t worker_index)noexcept;

    std::size_t ThreadCount()const noexcept;
    std::vector<std::size_t> ConnectionCounts()const;

private:
    std::size_t SelectRoundRobin();
    std::size_t SelectLeastConnections();

    EventLoop* base_loop_;
    EventLoopThreadPoolOptions options_;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;

    mutable std::mutex mutex_;
    std::vector<std::size_t> connection_counts_;
    std::size_t next_=0;
    bool started_=false;
};

}
