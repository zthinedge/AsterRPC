#pragma once

#include "asterrpc/protocol/RpcMessage.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <unordered_map>

namespace asterrpc::rpc{

class PendingCalls{
public:
    using ResponseFuture=std::future<protocol::RpcMessage>;
    using ResponseCallback=
        std::function<void(protocol::RpcMessage)>;

    ResponseFuture Add(std::uint64_t request_id);
    ResponseFuture Add(
        std::uint64_t request_id,
        std::uint64_t deadline_us
    );

    void Add(
        std::uint64_t request_id,
        ResponseCallback callback
    );
    void Add(
        std::uint64_t request_id,
        std::uint64_t deadline_us,
        ResponseCallback callback
    );

    bool SetTimeoutCancel(
        std::uint64_t request_id,
        std::function<void()> cancel
    );

    bool Complete(protocol::RpcMessage response);
    bool Expire(std::uint64_t request_id);

    bool Fail(
        std::uint64_t request_id,
        protocol::StatusCode status_code,
        const std::string& error_text
    );

    void FailAll(
        protocol::StatusCode status_code,
        const std::string& error_text
    );

    std::size_t Size()const;

private:
    struct PendingCall{
        std::promise<protocol::RpcMessage> promise;
        ResponseCallback callback;
        std::function<void()> cancel_timeout;
        std::uint64_t deadline_us=0;
    };

    using CallMap=
        std::unordered_map<
            std::uint64_t,
            PendingCall
        >;

    void Insert(std::uint64_t request_id,PendingCall call);
    static void Finish(
        PendingCall call,
        protocol::RpcMessage response
    )noexcept;

    mutable std::mutex mutex_;
    CallMap calls_;
};

}
