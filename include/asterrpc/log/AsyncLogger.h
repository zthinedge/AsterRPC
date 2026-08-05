#pragma once

#include "asterrpc/log/LogRecord.h"
#include "asterrpc/log/LogSink.h"
#include "asterrpc/log/LoggerOptions.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

namespace asterrpc::log{

class AsyncLogger{
public:
    explicit AsyncLogger(LoggerOptions options={});
    AsyncLogger(
        LoggerOptions options,
        std::unique_ptr<LogSink> sink
    );
    ~AsyncLogger();

    AsyncLogger(const AsyncLogger&)=delete;
    AsyncLogger& operator=(const AsyncLogger&)=delete;
    AsyncLogger(AsyncLogger&&)=delete;
    AsyncLogger& operator=(AsyncLogger&&)=delete;

    bool ShouldLog(LogLevel level)const noexcept;

    bool Log(
        LogLevel level,
        std::string_view file,
        int line,
        std::string_view function,
        std::string_view message
    );

    void Stop()noexcept;
    LoggerStats GetStats()const noexcept;

private:
    void WorkerLoop()noexcept;
    void TakeBatch(std::vector<LogRecord>* batch);
    void WriteBatch(const std::vector<LogRecord>& batch)noexcept;
    void WriteDropReport()noexcept;
    void UpdateQueuePeak(std::size_t size)noexcept;

    LoggerOptions options_;
    std::unique_ptr<LogSink> sink_;

    mutable std::mutex mutex_;
    std::condition_variable queue_ready_;
    std::deque<LogRecord> queue_;
    bool stopping_;

    std::mutex stop_mutex_;
    std::thread worker_;

    std::atomic_uint64_t enqueued_;
    std::atomic_uint64_t written_;
    std::atomic_uint64_t dropped_;
    std::atomic_uint64_t dropped_since_report_;
    std::atomic_uint64_t write_errors_;
    std::atomic_size_t queue_peak_;
};

}
