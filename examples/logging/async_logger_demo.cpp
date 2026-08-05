#include "asterrpc/log/AsyncLogger.h"
#include "asterrpc/log/LogMacros.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace asterrpc::log;

int main(){
    LoggerOptions options;
    options.file_path="logs/async_logger_demo.log";
    options.min_level=LogLevel::Info;
    options.queue_capacity=1024;
    options.batch_size=32;
    options.roll_size_bytes=8*1024;
    options.max_rolled_files=3;

    AsyncLogger logger(options);

    ASTERRPC_LOG_DEBUG(logger,"this debug log is filtered");
    ASTERRPC_LOG_INFO(logger,"async logger demo started");

    constexpr int thread_count=4;
    constexpr int logs_per_thread=100;
    std::vector<std::thread> threads;

    for(int thread_index=0;thread_index<thread_count;++thread_index){
        threads.emplace_back([&logger,thread_index](){
            for(int index=0;index<logs_per_thread;++index){
                ASTERRPC_LOG_INFO(
                    logger,
                    "worker="+std::to_string(thread_index)+
                    " record="+std::to_string(index)+
                    " processing rpc request"
                );
            }
        });
    }

    for(std::thread& thread:threads){
        thread.join();
    }

    ASTERRPC_LOG_WARN(logger,"async logger demo is stopping");
    logger.Stop();

    LoggerStats stats=logger.GetStats();
    std::cout<<"enqueued: "<<stats.enqueued<<'\n'
             <<"written:  "<<stats.written<<'\n'
             <<"dropped:  "<<stats.dropped<<'\n'
             <<"peak:     "<<stats.queue_peak<<"\n\n"
             <<"log files:\n";

    for(const auto& entry:std::filesystem::directory_iterator("logs")){
        std::string name=entry.path().filename().string();
        if(name.rfind("async_logger_demo",0)==0){
            std::cout<<"  "<<entry.path()<<'\n';
        }
    }

    return 0;
}
