#include "minirpc/net/EventLoop.h"
#include "minirpc/net/EventLoopThreadPool.h"

#include <cassert>
#include <vector>

using namespace minirpc::net;

namespace{

void TestRoundRobin(){
    EventLoop base_loop;
    EventLoopThreadPoolOptions options;
    options.thread_count=3;
    options.load_balance=IoLoopLoadBalance::RoundRobin;

    EventLoopThreadPool pool(&base_loop,options);
    pool.Start();

    auto first=pool.AcquireLoop();
    auto second=pool.AcquireLoop();
    auto third=pool.AcquireLoop();
    auto fourth=pool.AcquireLoop();

    assert(first.worker_index==0);
    assert(second.worker_index==1);
    assert(third.worker_index==2);
    assert(fourth.worker_index==0);
    assert(first.loop!=second.loop);
    assert(second.loop!=third.loop);

    std::vector<std::size_t> counts=pool.ConnectionCounts();
    assert((counts==std::vector<std::size_t>{2,1,1}));

    pool.ReleaseLoop(first.worker_index);
    pool.ReleaseLoop(second.worker_index);
    pool.ReleaseLoop(third.worker_index);
    pool.ReleaseLoop(fourth.worker_index);
    assert((pool.ConnectionCounts()==
            std::vector<std::size_t>{0,0,0}));
}

void TestLeastConnections(){
    EventLoop base_loop;
    EventLoopThreadPoolOptions options;
    options.thread_count=3;
    options.load_balance=IoLoopLoadBalance::LeastConnections;

    EventLoopThreadPool pool(&base_loop,options);
    pool.Start();

    auto first=pool.AcquireLoop();
    auto second=pool.AcquireLoop();
    auto third=pool.AcquireLoop();
    assert((pool.ConnectionCounts()==
            std::vector<std::size_t>{1,1,1}));

    pool.ReleaseLoop(first.worker_index);
    auto next=pool.AcquireLoop();
    assert(next.worker_index==first.worker_index);

    pool.ReleaseLoop(second.worker_index);
    pool.ReleaseLoop(third.worker_index);
    pool.ReleaseLoop(next.worker_index);
}

void TestZeroWorkersUseBaseLoop(){
    EventLoop base_loop;
    EventLoopThreadPool pool(&base_loop);
    pool.Start();

    auto selected=pool.AcquireLoop();
    assert(selected.loop==&base_loop);
    assert(selected.worker_index==
           EventLoopThreadPool::kBaseLoopIndex);
}

}

int main(){
    TestRoundRobin();
    TestLeastConnections();
    TestZeroWorkersUseBaseLoop();
    return 0;
}
