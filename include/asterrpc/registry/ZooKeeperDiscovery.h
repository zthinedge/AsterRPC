#pragma once

#include "asterrpc/cluster/Endpoint.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace asterrpc::registry{

class ZooKeeperClient;

enum class DiscoveryStatus{
    Ok,
    NotReady,
    NoProvider
};

using ProviderList=std::vector<cluster::Endpoint>;
using ProviderSnapshot=std::shared_ptr<const ProviderList>;

struct DiscoveryResult{
    DiscoveryStatus status=DiscoveryStatus::NotReady;
    ProviderSnapshot providers;
};

class ZooKeeperDiscovery{
public:
    using ErrorCallback=std::function<void(const std::string&)>;

    explicit ZooKeeperDiscovery(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path="/aster-rpc/services"
    );
    ~ZooKeeperDiscovery();

    ZooKeeperDiscovery(const ZooKeeperDiscovery&)=delete;
    ZooKeeperDiscovery& operator=(const ZooKeeperDiscovery&)=delete;
    ZooKeeperDiscovery(ZooKeeperDiscovery&&)=delete;
    ZooKeeperDiscovery& operator=(ZooKeeperDiscovery&&)=delete;

    void WatchService(const std::string& service_name);
    DiscoveryResult Resolve(const std::string& service_name)const;

    void SetErrorCallback(ErrorCallback callback);

private:
    class State;
    std::shared_ptr<State> state_;
};

}
