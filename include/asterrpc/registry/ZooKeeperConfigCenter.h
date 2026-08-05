#pragma once

#include "asterrpc/config/RpcConfig.h"

#include <functional>
#include <memory>
#include <string>

namespace asterrpc::registry{

class ZooKeeperClient;

class ZooKeeperConfigCenter{
public:
    using Snapshot=config::ConfigStore::Snapshot;
    using ListenerId=config::ConfigStore::ListenerId;
    using UpdateCallback=config::ConfigStore::UpdateCallback;
    using ErrorCallback=std::function<void(const std::string&)>;

    explicit ZooKeeperConfigCenter(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path="/aster-rpc/config",
        config::RpcConfig defaults={}
    );
    ~ZooKeeperConfigCenter();

    ZooKeeperConfigCenter(const ZooKeeperConfigCenter&)=delete;
    ZooKeeperConfigCenter& operator=(
        const ZooKeeperConfigCenter&
    )=delete;
    ZooKeeperConfigCenter(ZooKeeperConfigCenter&&)=delete;
    ZooKeeperConfigCenter& operator=(
        ZooKeeperConfigCenter&&
    )=delete;

    void WatchService(const std::string& service_name);
    Snapshot Get(const std::string& service_name)const;

    ListenerId Subscribe(
        std::string service_name,
        UpdateCallback callback
    );
    void Unsubscribe(ListenerId listener_id)noexcept;

    void SetErrorCallback(ErrorCallback callback);

    static std::string GlobalPath(const std::string& root_path);
    static std::string ServicePath(
        const std::string& root_path,
        const std::string& service_name
    );

private:
    class State;
    std::shared_ptr<State> state_;
};

}
