#pragma once

#include "asterrpc/log/AsyncLogger.h"

#define ASTERRPC_LOG(logger,level,message)                              \
    do{                                                               \
        auto& asterrpc_logger_=(logger);                               \
        if(asterrpc_logger_.ShouldLog(level)){                         \
            asterrpc_logger_.Log(                                     \
                level,__FILE__,__LINE__,__func__,message             \
            );                                                        \
        }                                                             \
    }while(false)

#define ASTERRPC_LOG_TRACE(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Trace,message)

#define ASTERRPC_LOG_DEBUG(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Debug,message)

#define ASTERRPC_LOG_INFO(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Info,message)

#define ASTERRPC_LOG_WARN(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Warn,message)

#define ASTERRPC_LOG_ERROR(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Error,message)

#define ASTERRPC_LOG_FATAL(logger,message) \
    ASTERRPC_LOG(logger,::asterrpc::log::LogLevel::Fatal,message)
