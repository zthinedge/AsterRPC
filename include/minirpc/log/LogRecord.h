#pragma once

#include "minirpc/log/LogLevel.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

namespace minirpc::log{

struct LogRecord{
    LogLevel level=LogLevel::Info;
    std::chrono::system_clock::time_point timestamp;
    std::thread::id thread_id;
    std::string file;
    int line=0;
    std::string function;
    std::string message;
    std::string trace_id;
    std::string span_id;
    std::string parent_span_id;
    std::uint64_t deadline_us=0;
};

}
