#include "asterrpc/cluster/ConnectionPool.h"
#include "asterrpc/cluster/Endpoint.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/protocol/RpcMessage.h"
#include "asterrpc/protocol/RpcMeta.h"
#include "asterrpc/rpc/CallOptions.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace{

using Clock=std::chrono::steady_clock;

constexpr const char* kBenchService="BenchService";
constexpr const char* kEchoMethod="Echo";

struct BenchOptions{
    std::string host="127.0.0.1";
    std::uint16_t port=9000;
    std::size_t concurrency=1;
    std::size_t requests=1000;
    std::size_t payload_size=64;
    std::size_t io_threads=1;
    std::size_t connections=4;
    std::size_t timeout_ms=5000;
};

std::size_t ParseSize(
    const std::string& text,
    const std::string& option
){
    if(text.empty()||text.front()=='-'){
        throw std::invalid_argument(
            option+" requires a non-negative integer"
        );
    }

    std::size_t parsed=0;
    unsigned long long value=0;

    try{
        value=std::stoull(text,&parsed);
    }catch(const std::exception&){
        throw std::invalid_argument(
            option+" requires a non-negative integer"
        );
    }

    if(parsed!=text.size()||
       value>std::numeric_limits<std::size_t>::max()){
        throw std::invalid_argument(
            option+" requires a valid integer"
        );
    }

    return static_cast<std::size_t>(value);
}

std::uint16_t ParsePort(const std::string& text){
    std::size_t value=ParseSize(text,"--port");
    if(value==0||value>65535){
        throw std::invalid_argument(
            "--port must be between 1 and 65535"
        );
    }
    return static_cast<std::uint16_t>(value);
}

void PrintUsage(const char* program){
    std::cout
        <<"Usage: "<<program<<" [options]\n"
        <<"  --host HOST          server host (default 127.0.0.1)\n"
        <<"  --port PORT          server port (default 9000)\n"
        <<"  --concurrency N      concurrent calls (default 1)\n"
        <<"  --requests N         total calls (default 1000)\n"
        <<"  --payload N          payload bytes (default 64)\n"
        <<"  --io-threads N       client EventLoops (default 1)\n"
        <<"  --connections N      total connections (default 4)\n"
        <<"  --timeout-ms N       call timeout, 0 disables it "
          "(default 5000)\n"
        <<"  --help               show this help\n";
}

BenchOptions ParseArguments(int argc,char* argv[]){
    BenchOptions options;

    for(int index=1;index<argc;++index){
        std::string argument=argv[index];
        if(argument=="--help"){
            PrintUsage(argv[0]);
            std::exit(0);
        }

        if(index+1>=argc){
            throw std::invalid_argument(
                "missing value for "+argument
            );
        }

        std::string value=argv[++index];
        if(argument=="--host"){
            options.host=std::move(value);
        }else if(argument=="--port"){
            options.port=ParsePort(value);
        }else if(argument=="--concurrency"){
            options.concurrency=ParseSize(value,argument);
        }else if(argument=="--requests"){
            options.requests=ParseSize(value,argument);
        }else if(argument=="--payload"){
            options.payload_size=ParseSize(value,argument);
        }else if(argument=="--io-threads"){
            options.io_threads=ParseSize(value,argument);
        }else if(argument=="--connections"){
            options.connections=ParseSize(value,argument);
        }else if(argument=="--timeout-ms"){
            options.timeout_ms=ParseSize(value,argument);
        }else{
            throw std::invalid_argument(
                "unknown option: "+argument
            );
        }
    }

    if(options.host.empty()){
        throw std::invalid_argument("--host must not be empty");
    }
    if(options.concurrency==0){
        throw std::invalid_argument(
            "--concurrency must be positive"
        );
    }
    if(options.requests==0){
        throw std::invalid_argument("--requests must be positive");
    }
    if(options.concurrency>10000){
        throw std::invalid_argument(
            "--concurrency must not exceed 10000"
        );
    }
    if(options.payload_size>64*1024*1024){
        throw std::invalid_argument(
            "--payload must not exceed 64 MiB"
        );
    }
    if(options.io_threads==0){
        throw std::invalid_argument(
            "--io-threads must be positive"
        );
    }
    if(options.io_threads>options.concurrency){
        throw std::invalid_argument(
            "--io-threads must not exceed concurrency"
        );
    }
    if(options.connections<options.io_threads){
        throw std::invalid_argument(
            "--connections must be at least io-threads"
        );
    }

    return options;
}

class LoopThread{
public:
    LoopThread(){
        std::promise<asterrpc::net::EventLoop*> ready;
        auto future=ready.get_future();

        thread_=std::thread([ready=std::move(ready)]()mutable{
            asterrpc::net::EventLoop loop;
            ready.set_value(&loop);
            loop.Loop();
        });
        loop_=future.get();
    }

    ~LoopThread(){
        if(thread_.joinable()){
            loop_->Stop();
            thread_.join();
        }
    }

    LoopThread(const LoopThread&)=delete;
    LoopThread& operator=(const LoopThread&)=delete;

    asterrpc::net::EventLoop* Loop()const noexcept{
        return loop_;
    }

    void Drain(){
        std::promise<void> done;
        auto future=done.get_future();
        loop_->RunInLoop([&done](){
            done.set_value();
        });
        future.get();
    }

private:
    asterrpc::net::EventLoop* loop_=nullptr;
    std::thread thread_;
};

std::uint64_t Percentile(
    const std::vector<std::uint64_t>& sorted,
    std::size_t percent
){
    if(sorted.empty()){
        return 0;
    }

    std::size_t rank=
        (sorted.size()/100)*percent+
        ((sorted.size()%100)*percent+99)/100;
    return sorted[rank-1];
}

struct BenchResult{
    std::size_t succeeded=0;
    std::size_t failed=0;
    std::chrono::microseconds elapsed{0};
    std::uint64_t average_latency_us=0;
    std::uint64_t min_latency_us=0;
    std::uint64_t max_latency_us=0;
    std::uint64_t p50_latency_us=0;
    std::uint64_t p95_latency_us=0;
    std::uint64_t p99_latency_us=0;
    std::string first_error;
};

struct BenchTarget{
    asterrpc::cluster::ConnectionPool* pool=nullptr;
    asterrpc::net::EventLoop* loop=nullptr;
};

class AsyncBenchmark{
public:
    AsyncBenchmark(
        const BenchOptions& options,
        std::vector<BenchTarget> targets
    ):options_(options),
      targets_(std::move(targets)),
      payload_(options.payload_size,'x'),
      latencies_(options.requests){
        call_options_.idempotent=true;
        call_options_.timeout=std::chrono::milliseconds(
            options.timeout_ms
        );
    }

    BenchResult Run(){
        std::size_t initial=std::min(
            options_.concurrency,
            options_.requests
        );
        next_request_.store(initial,std::memory_order_relaxed);

        auto benchmark_started=Clock::now();
        for(std::size_t index=0;index<initial;++index){
            Submit(targets_[index%targets_.size()],index);
        }

        {
            std::unique_lock<std::mutex> lock(done_mutex_);
            done_.wait(lock,[this](){
                return completed_.load(std::memory_order_acquire)==
                       options_.requests;
            });
        }
        auto benchmark_finished=Clock::now();

        std::sort(latencies_.begin(),latencies_.end());
        return MakeResult(benchmark_started,benchmark_finished);
    }

private:
    void Submit(
        BenchTarget target,
        std::size_t request_index
    ){
        auto request_started=Clock::now();

        try{
            target.pool->AsyncCall(
                kBenchService,
                kEchoMethod,
                payload_,
                [
                    this,
                    target,
                    request_index,
                    request_started
                ](asterrpc::protocol::RpcMessage response){
                    bool valid=
                        response.meta.status_code==
                            asterrpc::protocol::StatusCode::Ok&&
                        response.payload==payload_;
                    std::string error;
                    if(!valid){
                        error=response.meta.error_text.empty()
                            ?"invalid echo response"
                            :std::move(response.meta.error_text);
                    }
                    Complete(
                        target,
                        request_index,
                        request_started,
                        valid,
                        std::move(error)
                    );
                },
                call_options_
            );
        }catch(const std::exception& error){
            Complete(
                target,
                request_index,
                request_started,
                false,
                error.what()
            );
        }
    }

    void Complete(
        BenchTarget target,
        std::size_t request_index,
        Clock::time_point request_started,
        bool succeeded,
        std::string error
    ){
        latencies_[request_index]=static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now()-request_started
            ).count()
        );

        if(succeeded){
            succeeded_.fetch_add(1,std::memory_order_relaxed);
        }else{
            failed_.fetch_add(1,std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(error_mutex_);
            if(first_error_.empty()){
                first_error_=std::move(error);
            }
        }

        std::size_t next=
            next_request_.fetch_add(1,std::memory_order_relaxed);
        if(next<options_.requests){
            target.loop->QueueInLoop([this,target,next](){
                Submit(target,next);
            });
        }

        std::size_t completed=
            completed_.fetch_add(1,std::memory_order_acq_rel)+1;
        if(completed==options_.requests){
            done_.notify_one();
        }
    }

    BenchResult MakeResult(
        Clock::time_point benchmark_started,
        Clock::time_point benchmark_finished
    ){
        std::uint64_t total_latency=0;
        for(std::uint64_t latency:latencies_){
            total_latency+=latency;
        }

        BenchResult result;
        result.succeeded=succeeded_.load(std::memory_order_relaxed);
        result.failed=failed_.load(std::memory_order_relaxed);
        result.elapsed=
            std::chrono::duration_cast<std::chrono::microseconds>(
                benchmark_finished-benchmark_started
            );
        result.average_latency_us=
            total_latency/static_cast<std::uint64_t>(
                latencies_.size()
            );
        result.min_latency_us=latencies_.front();
        result.max_latency_us=latencies_.back();
        result.p50_latency_us=Percentile(latencies_,50);
        result.p95_latency_us=Percentile(latencies_,95);
        result.p99_latency_us=Percentile(latencies_,99);
        result.first_error=std::move(first_error_);
        return result;
    }

    const BenchOptions& options_;
    std::vector<BenchTarget> targets_;
    std::string payload_;
    std::vector<std::uint64_t> latencies_;
    asterrpc::rpc::CallOptions call_options_;

    std::atomic_size_t next_request_{0};
    std::atomic_size_t completed_{0};
    std::atomic_size_t succeeded_{0};
    std::atomic_size_t failed_{0};

    std::mutex done_mutex_;
    std::condition_variable done_;
    std::mutex error_mutex_;
    std::string first_error_;
};

void PrintResult(
    const BenchOptions& options,
    std::size_t connections,
    const BenchResult& result
){
    double elapsed_seconds=
        static_cast<double>(result.elapsed.count())/1000000.0;
    double qps=elapsed_seconds>0.0
        ?static_cast<double>(options.requests)/elapsed_seconds
        :0.0;

    std::cout<<std::fixed<<std::setprecision(2)
             <<"\nrpc_bench result\n"
             <<"endpoint: "<<options.host<<':'<<options.port<<'\n'
             <<"concurrency: "<<options.concurrency<<'\n'
             <<"client io threads: "<<options.io_threads<<'\n'
             <<"connections: "<<connections<<'\n'
             <<"timeout(ms): "<<options.timeout_ms<<'\n'
             <<"requests: "<<options.requests<<'\n'
             <<"payload bytes: "<<options.payload_size<<'\n'
             <<"succeeded: "<<result.succeeded<<'\n'
             <<"failed: "<<result.failed<<'\n'
             <<"elapsed(s): "<<elapsed_seconds<<'\n'
             <<"QPS: "<<qps<<'\n'
             <<"latency(us) avg/min/max: "
             <<result.average_latency_us<<'/'
             <<result.min_latency_us<<'/'
             <<result.max_latency_us<<'\n'
             <<"latency(us) P50/P95/P99: "
             <<result.p50_latency_us<<'/'
             <<result.p95_latency_us<<'/'
             <<result.p99_latency_us<<'\n';

    if(!result.first_error.empty()){
        std::cout<<"first error: "<<result.first_error<<'\n';
    }
}

}

int main(int argc,char* argv[]){
    try{
        BenchOptions options=ParseArguments(argc,argv);
        std::vector<std::unique_ptr<LoopThread>> loop_threads;
        std::vector<
            std::unique_ptr<asterrpc::cluster::ConnectionPool>
        > pools;
        std::vector<BenchTarget> targets;

        loop_threads.reserve(options.io_threads);
        pools.reserve(options.io_threads);
        targets.reserve(options.io_threads);

        for(std::size_t index=0;index<options.io_threads;++index){
            auto loop_thread=std::make_unique<LoopThread>();
            asterrpc::cluster::ConnectionPoolOptions pool_options;
            pool_options.max_connections=
                options.connections/options.io_threads+
                (index<options.connections%options.io_threads?1:0);
            pool_options.idle_timeout=
                std::chrono::milliseconds::zero();

            auto pool=
                std::make_unique<asterrpc::cluster::ConnectionPool>(
                    loop_thread->Loop(),
                    asterrpc::cluster::Endpoint(
                        options.host,
                        options.port
                    ),
                    pool_options
                );

            targets.push_back({pool.get(),loop_thread->Loop()});
            loop_threads.push_back(std::move(loop_thread));
            pools.push_back(std::move(pool));
        }

        BenchResult result;
        {
            AsyncBenchmark benchmark(options,std::move(targets));
            result=benchmark.Run();

            for(auto& loop_thread:loop_threads){
                loop_thread->Drain();
            }
        }
        PrintResult(options,options.connections,result);

        pools.clear();
        for(auto& loop_thread:loop_threads){
            loop_thread->Drain();
        }
        return result.failed==0?0:1;
    }catch(const std::exception& error){
        std::cerr<<"rpc_bench error: "<<error.what()<<'\n';
        PrintUsage(argv[0]);
        return 1;
    }
}
