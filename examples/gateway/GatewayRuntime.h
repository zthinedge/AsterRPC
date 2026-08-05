#pragma once

#include "asterrpc/gateway/AdminApi.h"
#include "asterrpc/gateway/RpcChannel.h"

#include <memory>

namespace google::protobuf{
class ServiceDescriptor;
}

namespace asterrpc::cluster{
class ChannelManager;
}

namespace asterrpc::log{
class AsyncLogger;
}

namespace asterrpc::net{
class EventLoop;
}

namespace asterrpc::registry{
class ZooKeeperClient;
class ZooKeeperConfigCenter;
class ZooKeeperDiscovery;
}

namespace asterrpc::example::gateway{

class GatewayRuntime final:
    public asterrpc::gateway::RpcChannel,
    public asterrpc::gateway::AdminDataSource{
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

    std::vector<asterrpc::gateway::AdminServiceInfo>
    Services()const override;
    std::vector<asterrpc::gateway::AdminInstanceInfo>
    Instances()const override;
    std::vector<asterrpc::gateway::AdminEndpointMetrics>
    Metrics()const override;
    std::vector<asterrpc::gateway::AdminServiceConfig>
    Config()const override;
    std::vector<asterrpc::gateway::AdminTraceInfo>
    Traces()const override;
    asterrpc::gateway::AdminHealthInfo Health()const override;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
};

}
