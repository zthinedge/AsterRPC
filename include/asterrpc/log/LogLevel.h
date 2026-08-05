#pragma once

#include <string_view>

namespace asterrpc::log{

enum class LogLevel{
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal
};

constexpr std::string_view ToString(LogLevel level)noexcept{
    switch(level){
    case LogLevel::Trace:
        return "TRACE";
    case LogLevel::Debug:
        return "DEBUG";
    case LogLevel::Info:
        return "INFO";
    case LogLevel::Warn:
        return "WARN";
    case LogLevel::Error:
        return "ERROR";
    case LogLevel::Fatal:
        return "FATAL";
    }

    return "UNKNOWN";
}

}
