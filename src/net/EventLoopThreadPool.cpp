#include "minirpc/net/EventLoopThreadPool.h"

#include "minirpc/net/EventLoop.h"
#include "minirpc/net/EventLoopThread.h"

#include <algorithm>
#include <stdexcept>

namespace minirpc::net{

EventLoopThreadPool::EventLoopThreadPool(
    EventLoop* base_loop,
    EventLoopThreadPoolOptions options
):base_loop_(base_loop),
  options_(options){
    if(base_loop_==nullptr){
        throw std::invalid_argument(
            "event loop thread pool base loop is null"
        );
    }
}

EventLoopThreadPool::~EventLoopThreadPool(){
    Stop();
}

void EventLoopThreadPool::Start(){
    std::lock_guard<std::mutex> lock(mutex_);
    if(started_){
        return;
    }

    threads_.reserve(options_.thread_count);
    loops_.reserve(options_.thread_count);
    connection_counts_.assign(options_.thread_count,0);

    try{
        for(std::size_t index=0;index<options_.thread_count;++index){
            auto thread=std::make_unique<EventLoopThread>();
            EventLoop* loop=thread->StartLoop();
            loops_.push_back(loop);
            threads_.push_back(std::move(thread));
        }
    }catch(...){
        for(auto& thread:threads_){
            thread->Stop();
        }
        threads_.clear();
        loops_.clear();
        connection_counts_.clear();
        throw;
    }

    next_=0;
    started_=true;
}

void EventLoopThreadPool::Stop()noexcept{
    std::vector<std::unique_ptr<EventLoopThread>> threads;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!started_&&threads_.empty()){
            return;
        }
        started_=false;
        loops_.clear();
        connection_counts_.clear();
        next_=0;
        threads.swap(threads_);
    }

    for(auto& thread:threads){
        thread->Stop();
    }
}

EventLoopThreadPool::Selection EventLoopThreadPool::AcquireLoop(){
    std::lock_guard<std::mutex> lock(mutex_);
    if(!started_){
        throw std::logic_error("event loop thread pool is not started");
    }

    if(loops_.empty()){
        return {base_loop_,kBaseLoopIndex};
    }

    std::size_t index=
        options_.load_balance==IoLoopLoadBalance::LeastConnections
        ?SelectLeastConnections()
        :SelectRoundRobin();

    ++connection_counts_[index];
    return {loops_[index],index};
}

void EventLoopThreadPool::ReleaseLoop(
    std::size_t worker_index
)noexcept{
    if(worker_index==kBaseLoopIndex){
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if(worker_index<connection_counts_.size()&&
       connection_counts_[worker_index]!=0){
        --connection_counts_[worker_index];
    }
}

std::size_t EventLoopThreadPool::ThreadCount()const noexcept{
    return options_.thread_count;
}

std::vector<std::size_t>
EventLoopThreadPool::ConnectionCounts()const{
    std::lock_guard<std::mutex> lock(mutex_);
    return connection_counts_;
}

std::size_t EventLoopThreadPool::SelectRoundRobin(){
    std::size_t index=next_;
    next_=(next_+1)%loops_.size();
    return index;
}

std::size_t EventLoopThreadPool::SelectLeastConnections(){
    std::size_t selected=next_;

    for(std::size_t offset=1;offset<loops_.size();++offset){
        std::size_t candidate=(next_+offset)%loops_.size();
        if(connection_counts_[candidate]<
           connection_counts_[selected]){
            selected=candidate;
        }
    }

    next_=(selected+1)%loops_.size();
    return selected;
}

}
