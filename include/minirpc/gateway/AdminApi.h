#pragma once

#include "minirpc/cluster/ConnectionPool.h"
#include "minirpc/config/RpcConfig.h"
#include "minirpc/gateway/HttpMessage.h"
#include "minirpc/health/EndpointState.h"
#include "minirpc/metrics/RpcMetrics.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace minirpc::gateway{

struct AdminServiceInfo{
    std::string name;
    std::vector<std::string> methods;
};

struct AdminInstanceInfo{
    std::string service;
    std::string endpoint;
    std::string discovery_status;
    cluster::ConnectionPoolStats pool;
    bool has_health_state=false;
    health::EndpointStateSnapshot health;
};

struct AdminEndpointMetrics{
    std::string endpoint;
    metrics::RpcMetricsSnapshot totals;
    std::vector<metrics::RpcMethodMetricsSnapshot> methods;
};

struct AdminServiceConfig{
    std::string service;
    config::RpcConfig config;
};

struct AdminTraceInfo{
    std::string trace_id;
    std::string service;
    std::string method;
    std::string endpoint;
    std::string status;
    std::uint64_t started_at_us=0;
    std::uint64_t latency_us=0;
};

struct AdminHealthInfo{
    bool ready=false;
    bool zookeeper_connected=false;
    std::size_t services=0;
    std::size_t instances=0;
    std::size_t selectable_instances=0;
};

class AdminDataSource{
public:
    virtual ~AdminDataSource()=default;

    virtual std::vector<AdminServiceInfo> Services()const=0;
    virtual std::vector<AdminInstanceInfo> Instances()const=0;
    virtual std::vector<AdminEndpointMetrics> Metrics()const=0;
    virtual std::vector<AdminServiceConfig> Config()const=0;
    virtual std::vector<AdminTraceInfo> Traces()const=0;
    virtual AdminHealthInfo Health()const=0;
};

class AdminApi{
public:
    explicit AdminApi(AdminDataSource* data_source);

    static bool Matches(const std::string& target)noexcept;
    HttpResponse Handle(const HttpRequest& request)const;

private:
    AdminDataSource* data_source_;
};

}
