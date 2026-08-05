#include "asterrpc/log/AsyncLogger.h"
#include "asterrpc/log/LogMacros.h"
#include "asterrpc/trace/TraceContext.h"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace asterrpc::log;

namespace{

class MemorySink:public LogSink{
public:
    bool Write(std::string_view bytes)override{
        std::lock_guard<std::mutex> lock(mutex_);
        contents_.append(bytes);
        return true;
    }

    bool Flush()override{
        return true;
    }

    std::string Contents()const{
        std::lock_guard<std::mutex> lock(mutex_);
        return contents_;
    }

private:
    mutable std::mutex mutex_;
    std::string contents_;
};

class BlockingSink:public LogSink{
public:
    bool Write(std::string_view)override{
        std::unique_lock<std::mutex> lock(mutex_);
        write_started_=true;
        write_started_condition_.notify_one();
        release_condition_.wait(lock,[this](){
            return released_;
        });
        return true;
    }

    bool Flush()override{
        return true;
    }

    void WaitUntilWriteStarts(){
        std::unique_lock<std::mutex> lock(mutex_);
        write_started_condition_.wait(lock,[this](){
            return write_started_;
        });
    }

    void Release(){
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_=true;
        }
        release_condition_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable write_started_condition_;
    std::condition_variable release_condition_;
    bool write_started_=false;
    bool released_=false;
};

LoggerOptions TestOptions(){
    LoggerOptions options;
    options.queue_capacity=1024;
    options.batch_size=64;
    options.flush_interval=std::chrono::milliseconds(10);
    return options;
}

std::filesystem::path MakeTempDirectory(){
    auto now=std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path directory=
        std::filesystem::temp_directory_path()/
        ("asterrpc-log-test-"+std::to_string(::getpid())+'-'+
         std::to_string(now));
    std::filesystem::create_directories(directory);
    return directory;
}

std::string ReadFile(const std::filesystem::path& path){
    std::ifstream input(path,std::ios::binary);
    std::ostringstream contents;
    contents<<input.rdbuf();
    return contents.str();
}

std::size_t CountLines(const std::string& contents){
    std::size_t count=0;
    for(char value:contents){
        if(value=='\n'){
            ++count;
        }
    }
    return count;
}

void TestLevelAndSourceMetadata(){
    auto sink=std::make_unique<MemorySink>();
    MemorySink* sink_ptr=sink.get();
    LoggerOptions options=TestOptions();
    options.min_level=LogLevel::Info;

    AsyncLogger logger(options,std::move(sink));
    assert(!logger.Log(
        LogLevel::Debug,__FILE__,__LINE__,__func__,"hidden"
    ));
    ASTERRPC_LOG_INFO(logger,"visible message");
    logger.Stop();

    std::string contents=sink_ptr->Contents();
    assert(contents.find("hidden")==std::string::npos);
    assert(contents.find("[INFO]")!=std::string::npos);
    assert(contents.find("visible message")!=std::string::npos);
    assert(contents.find("test_async_logger.cpp:")!=std::string::npos);
    assert(contents.find("TestLevelAndSourceMetadata")!=std::string::npos);

    LoggerStats stats=logger.GetStats();
    assert(stats.enqueued==1);
    assert(stats.written==1);
    assert(stats.dropped==0);
}

void TestConcurrentLogging(){
    auto sink=std::make_unique<MemorySink>();
    MemorySink* sink_ptr=sink.get();
    LoggerOptions options=TestOptions();
    options.queue_capacity=2048;

    AsyncLogger logger(options,std::move(sink));
    constexpr std::size_t thread_count=4;
    constexpr std::size_t records_per_thread=200;
    std::vector<std::thread> threads;

    for(std::size_t thread_index=0;
        thread_index<thread_count;
        ++thread_index){
        threads.emplace_back([&logger,thread_index](){
            for(std::size_t index=0;
                index<records_per_thread;
                ++index){
                bool accepted=logger.Log(
                    LogLevel::Info,
                    __FILE__,
                    __LINE__,
                    __func__,
                    "worker="+std::to_string(thread_index)+
                    " record="+std::to_string(index)
                );
                assert(accepted);
            }
        });
    }

    for(std::thread& thread:threads){
        thread.join();
    }
    logger.Stop();

    constexpr std::size_t expected=thread_count*records_per_thread;
    LoggerStats stats=logger.GetStats();
    assert(stats.enqueued==expected);
    assert(stats.written==expected);
    assert(stats.dropped==0);
    assert(CountLines(sink_ptr->Contents())==expected);
}

void TestTraceMetadata(){
    auto sink=std::make_unique<MemorySink>();
    MemorySink* sink_ptr=sink.get();
    AsyncLogger logger(TestOptions(),std::move(sink));

    asterrpc::trace::TraceContext context;
    context.trace_id="trace-123";
    context.span_id="span-456";
    context.parent_span_id="span-parent";
    context.deadline_us=789;

    {
        asterrpc::trace::TraceScope scope(context);
        ASTERRPC_LOG_INFO(logger,"traced message");
    }

    ASTERRPC_LOG_INFO(logger,"untraced message");
    logger.Stop();

    std::string contents=sink_ptr->Contents();
    assert(contents.find(
        "traced message [trace_id=trace-123 span_id=span-456 "
        "parent_span_id=span-parent deadline_us=789]"
    )!=std::string::npos);

    std::size_t untraced=contents.find("untraced message");
    assert(untraced!=std::string::npos);
    assert(contents.find("trace_id=",untraced)==std::string::npos);
}

void TestQueueFullDoesNotBlockProducer(){
    auto sink=std::make_unique<BlockingSink>();
    BlockingSink* sink_ptr=sink.get();
    LoggerOptions options=TestOptions();
    options.queue_capacity=2;
    options.batch_size=1;

    AsyncLogger logger(options,std::move(sink));
    assert(logger.Log(LogLevel::Info,"test.cpp",1,"test","first"));
    sink_ptr->WaitUntilWriteStarts();

    assert(logger.Log(LogLevel::Info,"test.cpp",2,"test","second"));
    assert(logger.Log(LogLevel::Info,"test.cpp",3,"test","third"));
    assert(!logger.Log(LogLevel::Info,"test.cpp",4,"test","dropped"));

    sink_ptr->Release();
    logger.Stop();

    LoggerStats stats=logger.GetStats();
    assert(stats.enqueued==3);
    assert(stats.written==3);
    assert(stats.dropped==1);
    assert(stats.queue_peak==2);
}

void TestRollingFiles(){
    std::filesystem::path directory=MakeTempDirectory();
    LoggerOptions options=TestOptions();
    options.file_path=directory/"service.log";
    options.batch_size=1;
    options.roll_size_bytes=180;
    options.max_rolled_files=2;

    {
        AsyncLogger logger(options);
        for(int index=0;index<20;++index){
            assert(logger.Log(
                LogLevel::Info,
                "rolling_test.cpp",
                index,
                "TestRollingFiles",
                "a message large enough to trigger rolling"
            ));
        }
        logger.Stop();
        assert(logger.GetStats().written==20);
    }

    std::size_t rolled_files=0;
    for(const auto& entry:std::filesystem::directory_iterator(directory)){
        std::string name=entry.path().filename().string();
        if(name!="service.log"&&name.rfind("service.",0)==0){
            ++rolled_files;
        }
    }

    assert(std::filesystem::exists(options.file_path));
    assert(rolled_files>=1);
    assert(rolled_files<=options.max_rolled_files);
    assert(!ReadFile(options.file_path).empty());
    std::filesystem::remove_all(directory);
}

}

int main(){
    TestLevelAndSourceMetadata();
    TestConcurrentLogging();
    TestTraceMetadata();
    TestQueueFullDoesNotBlockProducer();
    TestRollingFiles();
    return 0;
}
