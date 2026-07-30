#include "minirpc/common/ThreadPool.h"

#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <mutex>

using minirpc::common::ThreadPool;

namespace{

void Check(bool condition){
    if(!condition){
        std::abort();
    }
}

void TestWorkersRunTasks(){
    ThreadPool pool(2,8);

    std::mutex mutex;
    std::condition_variable ready;
    std::size_t completed=0;

    for(std::size_t index=0;index<8;++index){
        bool submitted=pool.TrySubmit([&](){
            {
                std::lock_guard<std::mutex> lock(mutex);
                ++completed;
            }
            ready.notify_one();
        });
        Check(submitted);
    }

    std::unique_lock<std::mutex> lock(mutex);
    ready.wait(lock,[&](){
        return completed==8;
    });
}

void TestBoundedQueueRejectsOverflow(){
    ThreadPool pool(1,1);

    std::mutex mutex;
    std::condition_variable ready;
    bool running=false;
    bool release=false;

    bool first_submitted=pool.TrySubmit([&](){
        std::unique_lock<std::mutex> lock(mutex);
        running=true;
        ready.notify_one();
        ready.wait(lock,[&](){
            return release;
        });
    });
    Check(first_submitted);

    {
        std::unique_lock<std::mutex> lock(mutex);
        ready.wait(lock,[&](){
            return running;
        });
    }

    bool second_submitted=pool.TrySubmit([](){});
    bool third_submitted=pool.TrySubmit([](){});
    Check(second_submitted);
    Check(!third_submitted);

    {
        std::lock_guard<std::mutex> lock(mutex);
        release=true;
    }
    ready.notify_one();
}

}

int main(){
    TestWorkersRunTasks();
    TestBoundedQueueRejectsOverflow();
    return 0;
}
