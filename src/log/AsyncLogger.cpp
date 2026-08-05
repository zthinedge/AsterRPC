#include "asterrpc/log/AsyncLogger.h"

#include "asterrpc/trace/TraceContext.h"
#include "RollingFileSink.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace asterrpc::log{
namespace{

void ValidateOptions(const LoggerOptions& options){
    if(options.queue_capacity==0){
        throw std::invalid_argument("log queue capacity must be positive");
    }
    if(options.batch_size==0){
        throw std::invalid_argument("log batch size must be positive");
    }
    if(options.flush_interval<=std::chrono::milliseconds::zero()){
        throw std::invalid_argument("log flush interval must be positive");
    }
}

std::unique_ptr<LogSink> CreateRollingSink(
    const LoggerOptions& options
){
    ValidateOptions(options);
    return std::make_unique<RollingFileSink>(
        options.file_path,
        options.roll_size_bytes,
        options.max_rolled_files
    );
}

std::tm ToLocalTime(std::time_t time){
    std::tm result{};
    ::localtime_r(&time,&result);
    return result;
}

std::string_view BaseName(std::string_view path){
    std::size_t position=path.find_last_of("/\\");
    if(position==std::string_view::npos){
        return path;
    }
    return path.substr(position+1);
}

void AppendRecord(std::ostringstream& output,const LogRecord& record){
    auto micros_since_epoch=
        std::chrono::duration_cast<std::chrono::microseconds>(
            record.timestamp.time_since_epoch()
        ).count();
    auto micros=micros_since_epoch%1000000;
    std::time_t time=std::chrono::system_clock::to_time_t(record.timestamp);
    std::tm local_time=ToLocalTime(time);

    output<<std::put_time(&local_time,"%Y-%m-%d %H:%M:%S")<<'.'
          <<std::setw(6)<<std::setfill('0')<<micros
          <<" ["<<ToString(record.level)<<']'
          <<" [tid="<<record.thread_id<<']'
          <<" ["<<BaseName(record.file)<<':'<<record.line;

    if(!record.function.empty()){
        output<<' '<<record.function;
    }

    output<<"] "<<record.message;

    if(!record.trace_id.empty()){
        output<<" [trace_id="<<record.trace_id
              <<" span_id="<<record.span_id;

        if(!record.parent_span_id.empty()){
            output<<" parent_span_id="<<record.parent_span_id;
        }
        if(record.deadline_us!=0){
            output<<" deadline_us="<<record.deadline_us;
        }

        output<<']';
    }

    output<<'\n';
}

}

AsyncLogger::AsyncLogger(LoggerOptions options)
:AsyncLogger(
    options,
    CreateRollingSink(options)
){}

AsyncLogger::AsyncLogger(
    LoggerOptions options,
    std::unique_ptr<LogSink> sink
):options_(std::move(options)),
  sink_(std::move(sink)),
  stopping_(false),
  enqueued_(0),
  written_(0),
  dropped_(0),
  dropped_since_report_(0),
  write_errors_(0),
  queue_peak_(0){
    ValidateOptions(options_);
    if(!sink_){
        throw std::invalid_argument("log sink must not be null");
    }

    worker_=std::thread([this](){
        WorkerLoop();
    });
}

AsyncLogger::~AsyncLogger(){
    Stop();
}

bool AsyncLogger::ShouldLog(LogLevel level)const noexcept{
    return static_cast<int>(level)>=static_cast<int>(options_.min_level);
}

bool AsyncLogger::Log(
    LogLevel level,
    std::string_view file,
    int line,
    std::string_view function,
    std::string_view message
){
    if(!ShouldLog(level)){
        return false;
    }

    LogRecord record;
    record.level=level;
    record.timestamp=std::chrono::system_clock::now();
    record.thread_id=std::this_thread::get_id();
    record.file=file;
    record.line=line;
    record.function=function;
    record.message=message;

    const trace::TraceContext* context=
        trace::CurrentTraceContext();
    if(context!=nullptr){
        record.trace_id=context->trace_id;
        record.span_id=context->span_id;
        record.parent_span_id=context->parent_span_id;
        record.deadline_us=context->deadline_us;
    }

    std::size_t queue_size=0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_){
            return false;
        }
        if(queue_.size()>=options_.queue_capacity){
            dropped_.fetch_add(1,std::memory_order_relaxed);
            dropped_since_report_.fetch_add(1,std::memory_order_relaxed);
            return false;
        }

        queue_.push_back(std::move(record));
        queue_size=queue_.size();
    }

    enqueued_.fetch_add(1,std::memory_order_relaxed);
    UpdateQueuePeak(queue_size);
    queue_ready_.notify_one();
    return true;
}

void AsyncLogger::Stop()noexcept{
    std::lock_guard<std::mutex> stop_lock(stop_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_&&!worker_.joinable()){
            return;
        }
        stopping_=true;
    }

    queue_ready_.notify_one();
    if(worker_.joinable()){
        worker_.join();
    }
}

LoggerStats AsyncLogger::GetStats()const noexcept{
    LoggerStats stats;
    stats.enqueued=enqueued_.load(std::memory_order_relaxed);
    stats.written=written_.load(std::memory_order_relaxed);
    stats.dropped=dropped_.load(std::memory_order_relaxed);
    stats.write_errors=write_errors_.load(std::memory_order_relaxed);
    stats.queue_peak=queue_peak_.load(std::memory_order_relaxed);
    return stats;
}

void AsyncLogger::WorkerLoop()noexcept{
    std::vector<LogRecord> batch;
    batch.reserve(options_.batch_size);
    auto next_flush=std::chrono::steady_clock::now()+options_.flush_interval;

    while(true){
        bool should_stop=false;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_ready_.wait_until(lock,next_flush,[this](){
                return stopping_||!queue_.empty();
            });

            TakeBatch(&batch);
            should_stop=stopping_&&queue_.empty();
        }

        if(!batch.empty()){
            WriteBatch(batch);
            batch.clear();
        }
        WriteDropReport();

        auto now=std::chrono::steady_clock::now();
        if(now>=next_flush||should_stop){
            if(!sink_->Flush()){
                write_errors_.fetch_add(1,std::memory_order_relaxed);
            }
            next_flush=now+options_.flush_interval;
        }

        if(should_stop){
            break;
        }
    }
}

void AsyncLogger::TakeBatch(std::vector<LogRecord>* batch){
    std::size_t count=std::min(options_.batch_size,queue_.size());
    batch->reserve(count);

    for(std::size_t index=0;index<count;++index){
        batch->push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
}

void AsyncLogger::WriteBatch(
    const std::vector<LogRecord>& batch
)noexcept{
    try{
        std::ostringstream output;
        for(const LogRecord& record:batch){
            AppendRecord(output,record);
        }

        if(sink_->Write(output.str())){
            written_.fetch_add(batch.size(),std::memory_order_relaxed);
        }else{
            write_errors_.fetch_add(batch.size(),std::memory_order_relaxed);
        }
    }catch(...){
        write_errors_.fetch_add(batch.size(),std::memory_order_relaxed);
    }
}

void AsyncLogger::WriteDropReport()noexcept{
    std::uint64_t count=
        dropped_since_report_.exchange(0,std::memory_order_relaxed);
    if(count==0){
        return;
    }

    try{
        std::string message=
            "[asterrpc logger] dropped "+std::to_string(count)+
            " log records because the queue was full\n";
        if(!sink_->Write(message)){
            write_errors_.fetch_add(1,std::memory_order_relaxed);
        }
    }catch(...){
        write_errors_.fetch_add(1,std::memory_order_relaxed);
    }
}

void AsyncLogger::UpdateQueuePeak(std::size_t size)noexcept{
    std::size_t peak=queue_peak_.load(std::memory_order_relaxed);
    while(size>peak&&!queue_peak_.compare_exchange_weak(
        peak,size,std::memory_order_relaxed
    )){}
}

}
