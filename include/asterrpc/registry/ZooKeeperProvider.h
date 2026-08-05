#pragma once

#include "asterrpc/cluster/Endpoint.h"

#include <functional>
#include <memory>
#include <string>

namespace asterrpc::registry{

class ZooKeeperClient;

class ZooKeeperProvider{
public:
    using ErrorCallback=std::function<void(const std::string&)>;

    explicit ZooKeeperProvider(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path="/aster-rpc/services"
    );
    ~ZooKeeperProvider();

    ZooKeeperProvider(const ZooKeeperProvider&)=delete;
    ZooKeeperProvider& operator=(const ZooKeeperProvider&)=delete;
    ZooKeeperProvider(ZooKeeperProvider&&)=delete;
    ZooKeeperProvider& operator=(ZooKeeperProvider&&)=delete;

    void Register(
        std::string service_name,
        cluster::Endpoint endpoint
    );

    std::string RegisteredNode(
        const std::string& service_name,
        const cluster::Endpoint& endpoint
    )const;

    void SetErrorCallback(ErrorCallback callback);

    static std::string ProviderParentPath(
        const std::string& root_path,
        const std::string& service_name
    );

private:
    class State;
    std::shared_ptr<State> state_;
};

}
