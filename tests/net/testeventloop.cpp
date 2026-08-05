#include "asterrpc/net/EventLoop.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

using namespace asterrpc::net;

namespace{

void TestRunInLoop(){
    EventLoop loop;
    bool called=false;

    loop.RunInLoop([&called](){
        called=true;
    });

    assert(called);
    assert(loop.IsInLoopThread());
}

void TestCrossThreadQueue(){
    std::promise<EventLoop*>loop_ready;
    std::promise<void>tasks_done;

    std::thread loop_thread([&loop_ready](){
        EventLoop loop;
        loop_ready.set_value(&loop);
        loop.Loop();
    });

    EventLoop*loop=loop_ready.get_future().get();
    assert(!loop->IsInLoopThread());

    constexpr int producer_count=4;
    constexpr int tasks_per_producer=100;
    std::atomic_int executed{0};
    std::atomic_bool all_in_loop_thread{true};
    std::vector<std::thread>producers;

    for(int i=0;i<producer_count;++i){
        producers.emplace_back([loop,&executed,&all_in_loop_thread](){
            for(int j=0;j<tasks_per_producer;++j){
                loop->QueueInLoop([loop,&executed,&all_in_loop_thread](){
                    if(!loop->IsInLoopThread()){
                        all_in_loop_thread.store(false);
                    }
                    executed.fetch_add(1);
                });
            }
        });
    }

    for(auto&producer:producers){
        producer.join();
    }

    loop->RunInLoop([&](){
        assert(executed.load()==producer_count*tasks_per_producer);
        assert(all_in_loop_thread.load());
        tasks_done.set_value();
        loop->Stop();
    });

    assert(tasks_done.get_future().wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    loop_thread.join();
}

void TestStopWakesPoller(){
    std::promise<EventLoop*>loop_ready;

    std::thread loop_thread([&loop_ready](){
        EventLoop loop;
        loop_ready.set_value(&loop);
        loop.Loop();
    });

    EventLoop*loop=loop_ready.get_future().get();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto start=std::chrono::steady_clock::now();
    loop->Stop();
    loop_thread.join();
    auto elapsed=std::chrono::steady_clock::now()-start;

    assert(elapsed<std::chrono::seconds(1));
}

void TestTimerQueue(){
    EventLoop loop;
    bool fired=false;
    bool cancelled_fired=false;

    EventLoop::TimerId cancelled=loop.RunAfter(
        std::chrono::milliseconds(5),
        [&cancelled_fired](){
            cancelled_fired=true;
        }
    );
    loop.CancelTimer(cancelled);

    auto start=std::chrono::steady_clock::now();
    loop.RunAfter(std::chrono::milliseconds(20),[&](){
        fired=true;
        loop.Stop();
    });
    loop.Loop();

    auto elapsed=std::chrono::steady_clock::now()-start;
    assert(fired);
    assert(!cancelled_fired);
    assert(elapsed>=std::chrono::milliseconds(15));
}

void TestCrossThreadTimer(){
    std::promise<EventLoop*>loop_ready;
    std::promise<bool>timer_done;

    std::thread loop_thread([&loop_ready](){
        EventLoop loop;
        loop_ready.set_value(&loop);
        loop.Loop();
    });

    EventLoop*loop=loop_ready.get_future().get();
    loop->RunAfter(std::chrono::milliseconds(10),[loop,&timer_done](){
        timer_done.set_value(loop->IsInLoopThread());
        loop->Stop();
    });

    auto result=timer_done.get_future();
    assert(result.wait_for(std::chrono::seconds(2))==
           std::future_status::ready);
    assert(result.get());
    loop_thread.join();
}

}

int main(){
    TestRunInLoop();
    TestCrossThreadQueue();
    TestStopWakesPoller();
    TestTimerQueue();
    TestCrossThreadTimer();
    return 0;
}
