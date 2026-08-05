#pragma once

#include "asterrpc/cluster/ConnectionPool.h"
#include "asterrpc/cluster/Endpoint.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace asterrpc::net{
class EventLoop;
}

namespace asterrpc::cluster{

class ChannelManager{
public:
    explicit ChannelManager(net::EventLoop* loop);

    std::shared_ptr<ConnectionPool> GetOrCreate(
        const Endpoint& endpoint,
        ConnectionPoolOptions options={}
    );

    std::shared_ptr<ConnectionPool> Find(
        const Endpoint& endpoint
    )const;

    bool Remove(const Endpoint& endpoint);
    std::size_t Size()const;
    std::vector<std::shared_ptr<ConnectionPool>>
    Snapshot()const;

private:
    net::EventLoop* loop_;
    mutable std::mutex mutex_;
    std::unordered_map<
        Endpoint,
        std::shared_ptr<ConnectionPool>,
        EndpointHash
    > pools_;
};

}
