#include "minirpc/net/EventLoopThread.h"

#include "minirpc/net/EventLoop.h"

#include <stdexcept>

namespace minirpc::net{

EventLoopThread::~EventLoopThread(){
    Stop();
}

EventLoop* EventLoopThread::StartLoop(){
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(started_){
            throw std::logic_error("event loop thread already started");
        }
        started_=true;
        startup_error_=nullptr;
    }

    thread_=std::thread([this](){
        ThreadMain();
    });

    std::unique_lock<std::mutex> lock(mutex_);
    ready_.wait(lock,[this](){
        return loop_!=nullptr||startup_error_!=nullptr;
    });

    if(startup_error_!=nullptr){
        std::exception_ptr error=startup_error_;
        lock.unlock();
        if(thread_.joinable()){
            thread_.join();
        }
        std::rethrow_exception(error);
    }

    return loop_;
}

void EventLoopThread::Stop()noexcept{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(loop_!=nullptr){
            loop_->Stop();
        }
    }
    if(thread_.joinable()){
        thread_.join();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    loop_=nullptr;
    started_=false;
}

void EventLoopThread::ThreadMain()noexcept{
    try{
        EventLoop loop;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            loop_=&loop;
        }
        ready_.notify_one();

        loop.Loop();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            loop_=nullptr;
        }
    }catch(...){
        {
            std::lock_guard<std::mutex> lock(mutex_);
            loop_=nullptr;
            startup_error_=std::current_exception();
        }
        ready_.notify_one();
    }
}

}
