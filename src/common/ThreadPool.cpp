#include "minirpc/common/ThreadPool.h"

#include <stdexcept>
#include <utility>

namespace minirpc::common{

ThreadPool::ThreadPool(
    std::size_t thread_count,
    std::size_t queue_capacity
):queue_capacity_(queue_capacity){
    if(thread_count==0){
        throw std::invalid_argument(
            "thread pool must contain at least one thread"
        );
    }

    workers_.reserve(thread_count);
    try{
        for(std::size_t index=0;index<thread_count;++index){
            workers_.emplace_back([this](){
                WorkerLoop();
            });
        }
    }catch(...){
        Stop();
        throw;
    }
}

ThreadPool::~ThreadPool(){
    Stop();
}

bool ThreadPool::TrySubmit(Task task){
    if(!task){
        throw std::invalid_argument("thread pool task is empty");
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_){
            return false;
        }
        if(queue_capacity_!=0&&tasks_.size()>=queue_capacity_){
            return false;
        }
        tasks_.push(std::move(task));
    }

    ready_.notify_one();
    return true;
}

void ThreadPool::Stop()noexcept{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_){
            return;
        }
        stopping_=true;
    }

    ready_.notify_all();
    for(std::thread& worker:workers_){
        if(worker.joinable()){
            worker.join();
        }
    }
    workers_.clear();
}

std::size_t ThreadPool::ThreadCount()const noexcept{
    return workers_.size();
}

std::size_t ThreadPool::PendingTasks()const{
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
}

void ThreadPool::WorkerLoop()noexcept{
    while(true){
        Task task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ready_.wait(lock,[this](){
                return stopping_||!tasks_.empty();
            });

            if(tasks_.empty()){
                if(stopping_){
                    return;
                }
                continue;
            }

            task=std::move(tasks_.front());
            tasks_.pop();
        }

        try{
            task();
        }catch(...){
            // 单个业务任务不能终止工作线程。
        }
    }
}

}
