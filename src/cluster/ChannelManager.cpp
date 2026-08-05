#include "asterrpc/cluster/ChannelManager.h"

#include <stdexcept>

namespace asterrpc::cluster{

ChannelManager::ChannelManager(net::EventLoop* loop):loop_(loop){
    if(loop_==nullptr){
        throw std::invalid_argument("channel manager EventLoop is null");
    }
}

std::shared_ptr<ConnectionPool> ChannelManager::GetOrCreate(
    const Endpoint& endpoint,
    ConnectionPoolOptions options
){
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing=pools_.find(endpoint);

    if(existing!=pools_.end()){
        if(existing->second->Options()!=options){
            throw std::logic_error(
                "endpoint connection pool already has different options"
            );
        }
        return existing->second;
    }

    auto pool=std::make_shared<ConnectionPool>(
        loop_,endpoint,options
    );
    pools_.emplace(endpoint,pool);
    return pool;
}

std::shared_ptr<ConnectionPool> ChannelManager::Find(
    const Endpoint& endpoint
)const{
    std::lock_guard<std::mutex> lock(mutex_);
    auto pool=pools_.find(endpoint);
    return pool==pools_.end()?nullptr:pool->second;
}

bool ChannelManager::Remove(const Endpoint& endpoint){
    std::lock_guard<std::mutex> lock(mutex_);
    return pools_.erase(endpoint)!=0;
}

std::size_t ChannelManager::Size()const{
    std::lock_guard<std::mutex> lock(mutex_);
    return pools_.size();
}

std::vector<std::shared_ptr<ConnectionPool>>
ChannelManager::Snapshot()const{
    std::vector<std::shared_ptr<ConnectionPool>> pools;
    std::lock_guard<std::mutex> lock(mutex_);
    pools.reserve(pools_.size());
    for(const auto& entry:pools_){
        pools.push_back(entry.second);
    }
    return pools;
}

}
