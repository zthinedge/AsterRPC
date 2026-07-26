#pragma once

#include "minirpc/gateway/AdminApi.h"
#include "minirpc/gateway/RpcChannel.h"

#include <memory>

namespace google::protobuf{
class ServiceDescriptor;
}

namespace minirpc::cluster{
class ChannelManager;
}

namespace minirpc::log{
class AsyncLogger;
}

namespace minirpc::net{
class EventLoop;
}

namespace minirpc::registry{
class ZooKeeperClient;
class ZooKeeperConfigCenter;
class ZooKeeperDiscovery;
}

namespace minirpc::example::gateway{

class GatewayRuntime final:
    public minirpc::gateway::RpcChannel,
    public minirpc::gateway::AdminDataSource{
public:
    GatewayRuntime(
        std::shared_ptr<registry::ZooKeeperClient> zk_client,
        registry::ZooKeeperDiscovery* discovery,
        registry::ZooKeeperConfigCenter* config_center,
        cluster::ChannelManager* channels,
        log::AsyncLogger* logger,
        const google::protobuf::ServiceDescriptor* service
    );
    ~GatewayRuntime()override;

    GatewayRuntime(const GatewayRuntime&)=delete;
    GatewayRuntime& operator=(const GatewayRuntime&)=delete;

    void Start(net::EventLoop* loop);
    void Stop()noexcept;

    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions options,
        Completion completion
    )override;

    std::vector<minirpc::gateway::AdminServiceInfo>
    Services()const override;
    std::vector<minirpc::gateway::AdminInstanceInfo>
    Instances()const override;
    std::vector<minirpc::gateway::AdminEndpointMetrics>
    Metrics()const override;
    std::vector<minirpc::gateway::AdminServiceConfig>
    Config()const override;
    std::vector<minirpc::gateway::AdminTraceInfo>
    Traces()const override;
    minirpc::gateway::AdminHealthInfo Health()const override;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}
