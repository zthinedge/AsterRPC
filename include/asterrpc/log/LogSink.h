#pragma once

#include <string_view>

namespace asterrpc::log{

class LogSink{
public:
    virtual ~LogSink()=default;

    virtual bool Write(std::string_view bytes)=0;
    virtual bool Flush()=0;
};

}
