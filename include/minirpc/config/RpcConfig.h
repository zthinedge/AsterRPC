#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace minirpc::config{

enum class LoadBalancerAlgorithm{
    RoundRobin,
    P2cEwma
};

const char* ToString(LoadBalancerAlgorithm algorithm)noexcept;

struct RpcConfig{
    std::chrono::milliseconds default_timeout{1200};
    std::uint32_t retry_count=0;
    LoadBalancerAlgorithm load_balancer=
        LoadBalancerAlgorithm::P2cEwma;
    std::chrono::milliseconds health_check_interval{2000};
    std::size_t failure_threshold=3;
    double ewma_alpha=0.2;
};

struct RpcConfigPatch{
    std::optional<std::chrono::milliseconds> default_timeout;
    std::optional<std::uint32_t> retry_count;
    std::optional<LoadBalancerAlgorithm> load_balancer;
    std::optional<std::chrono::milliseconds> health_check_interval;
    std::optional<std::size_t> failure_threshold;
    std::optional<double> ewma_alpha;
};

void Validate(const RpcConfig& config);

RpcConfigPatch ParseRpcConfigPatch(std::string_view json);

RpcConfig Merge(
    const RpcConfig& base,
    const RpcConfigPatch& patch
);

class ConfigStore{
public:
    using Snapshot=std::shared_ptr<const RpcConfig>;
    using ListenerId=std::uint64_t;
    using UpdateCallback=std::function<void(Snapshot)>;

    explicit ConfigStore(RpcConfig defaults={});
    ~ConfigStore();

    ConfigStore(const ConfigStore&)=delete;
    ConfigStore& operator=(const ConfigStore&)=delete;
    ConfigStore(ConfigStore&&)=delete;
    ConfigStore& operator=(ConfigStore&&)=delete;

    Snapshot Get(const std::string& service_name)const;

    void UpdateGlobal(RpcConfigPatch patch);
    void UpdateService(
        std::string service_name,
        RpcConfigPatch patch
    );

    ListenerId Subscribe(
        std::string service_name,
        UpdateCallback callback
    );
    void Unsubscribe(ListenerId listener_id)noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
