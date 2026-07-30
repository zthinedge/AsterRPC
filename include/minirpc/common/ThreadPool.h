#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace minirpc::common{

class ThreadPool{
public:
    using Task=std::function<void()>;

    explicit ThreadPool(
        std::size_t thread_count,
        std::size_t queue_capacity=0
    );
    ~ThreadPool();

    ThreadPool(const ThreadPool&)=delete;
    ThreadPool& operator=(const ThreadPool&)=delete;
    ThreadPool(ThreadPool&&)=delete;
    ThreadPool& operator=(ThreadPool&&)=delete;

    bool TrySubmit(Task task);
    void Stop()noexcept;

    std::size_t ThreadCount()const noexcept;
    std::size_t PendingTasks()const;

private:
    void WorkerLoop()noexcept;

    const std::size_t queue_capacity_;
    std::vector<std::thread> workers_;

    mutable std::mutex mutex_;
    std::condition_variable ready_;
    std::queue<Task> tasks_;
    bool stopping_=false;
};

}
