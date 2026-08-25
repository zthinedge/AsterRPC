#pragma once

#include <chrono>
#include <cstdint>

namespace asterrpc::rpc{

struct CallOptions{
    // 相对超时；0表示不限制。
    std::chrono::microseconds timeout{0};

    // 绝对Unix时间，单位为微秒；0表示不限制。
    std::uint64_t deadline_us=0;

};

}
