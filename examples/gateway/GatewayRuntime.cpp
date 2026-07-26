#include "GatewayRuntime.h"

#include "minirpc/cluster/ChannelManager.h"
#include "minirpc/cluster/RetryPolicy.h"
#include "minirpc/cluster/RoundRobin.h"
#include "minirpc/config/RpcConfig.h"
#include "minirpc/health/HealthService.h"
#include "minirpc/loadbalance/P2cEwmaLoadBalancer.h"
#include "minirpc/log/AsyncLogger.h"
#include "minirpc/log/LogMacros.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/registry/ZooKeeperClient.h"
#include "minirpc/registry/ZooKeeperConfigCenter.h"
#include "minirpc/registry/ZooKeeperDiscovery.h"
#include "minirpc/trace/TraceContext.h"

#include <google/protobuf/descriptor.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace minirpc::example::gateway{
namespace{

protocol::RpcMessage ErrorResponse(
    protocol::StatusCode status,
    std::string message
){
    protocol::RpcMessage response;
    response.message_type=protocol::MessageType::Response;
    response.meta.status_code=status;
    response.meta.error_text=std::move(message);
    return response;
}

const char* DiscoveryStatusName(
    registry::DiscoveryStatus status
)noexcept{
    switch(status){
        case registry::DiscoveryStatus::Ok:
            return "ok";
        case registry::DiscoveryStatus::NotReady:
            return "not_ready";
        case registry::DiscoveryStatus::NoProvider:
            return "no_provider";
    }
    return "unknown";
}

const char* RpcStatusName(protocol::StatusCode status)noexcept{
    switch(status){
        case protocol::StatusCode::Ok:
            return "ok";
        case protocol::StatusCode::DecodeError:
            return "decode_error";
        case protocol::StatusCode::ServiceNotFound:
            return "service_not_found";
        case protocol::StatusCode::MethodNotFound:
            return "method_not_found";
        case protocol::StatusCode::InvokeError:
            return "invoke_error";
        case protocol::StatusCode::InternalError:
            return "internal_error";
        case protocol::StatusCode::Timeout:
            return "timeout";
        case protocol::StatusCode::ConnectionFailed:
            return "connection_failed";
    }
    return "unknown";
}

bool IsEndpointFailure(protocol::StatusCode status)noexcept{
    return status==protocol::StatusCode::Timeout||
           status==protocol::StatusCode::ConnectionFailed;
}

std::uint64_t UnixMicros()noexcept{
    auto now=std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            now
        ).count()
    );
}

}

class GatewayRuntime::Impl:
    public std::enable_shared_from_this<GatewayRuntime::Impl>{
public:
    Impl(
        std::shared_ptr<registry::ZooKeeperClient> zk_client,
        registry::ZooKeeperDiscovery* discovery,
        registry::ZooKeeperConfigCenter* config_center,
        cluster::ChannelManager* channels,
        log::AsyncLogger* logger,
        const google::protobuf::ServiceDescriptor* service
    ):zk_client_(std::move(zk_client)),
      discovery_(discovery),
      config_center_(config_center),
      channels_(channels),
      logger_(logger){
        if(zk_client_==nullptr||
           discovery_==nullptr||
           config_center_==nullptr||
           channels_==nullptr||
           logger_==nullptr||
           service==nullptr){
            throw std::invalid_argument(
                "gateway runtime dependency is null"
            );
        }

        minirpc::gateway::AdminServiceInfo info;
        info.name=std::string(
            service->full_name().data(),
            service->full_name().size()
        );
        for(int index=0;index<service->method_count();++index){
            const auto* method=service->method(index);
            info.methods.emplace_back(
                method->name().data(),
                method->name().size()
            );
        }
        services_.push_back(std::move(info));
    }

    ~Impl(){
        Stop();
    }

    void Start(net::EventLoop* loop){
        if(loop==nullptr){
            throw std::invalid_argument(
                "gateway runtime EventLoop is null"
            );
        }
        if(started_){
            throw std::logic_error(
                "gateway runtime already started"
            );
        }
        started_=true;

        std::weak_ptr<Impl> weak=shared_from_this();
        health::ActiveHealthCheckOptions health_options;
        health_options.interval=std::chrono::seconds(2);
        health_options.timeout=std::chrono::milliseconds(800);
        load_balancer_.GetHealthChecker().Start(
            loop,
            [weak](
                const cluster::Endpoint& endpoint,
                std::chrono::milliseconds timeout,
                health::HealthChecker::ProbeCompletion completion
            ){
                auto self=weak.lock();
                if(self==nullptr){
                    completion(false);
                    return;
                }

                rpc::CallOptions call_options;
                call_options.idempotent=true;
                call_options.timeout=timeout;
                try{
                    self->channels_->GetOrCreate(endpoint)->AsyncCall(
                        health::HealthService::ServiceName(),
                        health::HealthService::MethodName(),
                        {},
                        [completion](
                            protocol::RpcMessage response
                        ){
                            completion(
                                response.meta.status_code==
                                    protocol::StatusCode::Ok&&
                                response.payload==
                                    health::HealthService::
                                        ServingPayload()
                            );
                        },
                        call_options
                    );
                }catch(...){
                    completion(false);
                }
            },
            health_options
        );

        config_listener_=config_center_->Subscribe(
            services_.front().name,
            [weak](config::ConfigStore::Snapshot snapshot){
                if(auto self=weak.lock()){
                    self->ApplyConfig(*snapshot);
                }
            }
        );
    }

    void Stop()noexcept{
        if(!started_){
            return;
        }
        started_=false;
        load_balancer_.GetHealthChecker().Stop();
        config_center_->Unsubscribe(config_listener_);
        config_listener_=0;
    }

    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions options,
        minirpc::gateway::RpcChannel::Completion completion
    ){
        registry::DiscoveryResult discovered=
            discovery_->Resolve(service_name);
        auto runtime_config=config_center_->Get(service_name);

        std::optional<cluster::Endpoint> endpoint;
        std::shared_ptr<
            loadbalance::P2cEwmaLoadBalancer::Selection
        > p2c_selection;
        if(runtime_config->load_balancer==
           config::LoadBalancerAlgorithm::RoundRobin){
            endpoint=round_robin_.Select(discovered.providers);
        }else{
            auto selected=load_balancer_.Select(
                discovered.providers
            );
            if(selected){
                endpoint=selected->GetEndpoint();
                p2c_selection=std::make_shared<
                    loadbalance::P2cEwmaLoadBalancer::Selection
                >(std::move(*selected));
            }
        }

        const trace::TraceContext* context=
            trace::CurrentTraceContext();
        minirpc::gateway::AdminTraceInfo trace_info;
        trace_info.trace_id=
            context==nullptr?std::string{}:context->trace_id;
        trace_info.service=service_name;
        trace_info.method=method_name;
        trace_info.started_at_us=UnixMicros();
        auto started_at=std::chrono::steady_clock::now();

        if(!endpoint){
            protocol::RpcMessage response=ErrorResponse(
                protocol::StatusCode::ConnectionFailed,
                discovered.status==
                    registry::DiscoveryStatus::NoProvider
                ?"no provider for "+service_name
                :"service discovery is not ready"
            );
            trace_info.status=RpcStatusName(
                response.meta.status_code
            );
            RecordTrace(std::move(trace_info));
            completion(std::move(response));
            return;
        }
        trace_info.endpoint=endpoint->ToString();

        MINIRPC_LOG_INFO(
            *logger_,
            "HTTP gateway calling "+service_name+'.'+method_name+
            " endpoint="+endpoint->ToString()
        );

        options.timeout=runtime_config->default_timeout;
        cluster::RetryPolicy retry_policy(
            static_cast<std::size_t>(
                runtime_config->retry_count
            )+1,
            std::chrono::milliseconds(10)
        );

        auto self=shared_from_this();
        try{
            channels_->GetOrCreate(*endpoint)->AsyncCall(
                std::move(service_name),
                std::move(method_name),
                std::move(payload),
                [
                    self,
                    completion,
                    p2c_selection,
                    trace_info,
                    started_at
                ](protocol::RpcMessage response)mutable{
                    auto latency=
                        std::chrono::duration_cast<
                            std::chrono::microseconds
                        >(
                            std::chrono::steady_clock::now()-
                            started_at
                        );
                    if(p2c_selection!=nullptr){
                        if(IsEndpointFailure(
                               response.meta.status_code
                           )){
                            p2c_selection->CompleteFailure(latency);
                        }else{
                            p2c_selection->CompleteSuccess(latency);
                        }
                    }
                    trace_info.status=RpcStatusName(
                        response.meta.status_code
                    );
                    trace_info.latency_us=
                        static_cast<std::uint64_t>(
                            latency.count()
                        );
                    self->RecordTrace(std::move(trace_info));
                    completion(std::move(response));
                },
                options,
                retry_policy
            );
        }catch(const std::exception& error){
            if(p2c_selection!=nullptr){
                auto latency=
                    std::chrono::duration_cast<
                        std::chrono::microseconds
                    >(
                        std::chrono::steady_clock::now()-
                        started_at
                    );
                p2c_selection->CompleteFailure(latency);
            }
            protocol::RpcMessage response=ErrorResponse(
                protocol::StatusCode::ConnectionFailed,
                error.what()
            );
            trace_info.status=RpcStatusName(
                response.meta.status_code
            );
            RecordTrace(std::move(trace_info));
            completion(std::move(response));
        }
    }

    std::vector<minirpc::gateway::AdminServiceInfo>
    Services()const{
        return services_;
    }

    std::vector<minirpc::gateway::AdminInstanceInfo>
    Instances()const{
        std::vector<minirpc::gateway::AdminInstanceInfo> result;
        for(const auto& service:services_){
            auto discovered=discovery_->Resolve(service.name);
            load_balancer_.GetHealthChecker().Update(
                discovered.providers
            );
            for(const auto& endpoint:*discovered.providers){
                minirpc::gateway::AdminInstanceInfo instance;
                instance.service=service.name;
                instance.endpoint=endpoint.ToString();
                instance.discovery_status=
                    DiscoveryStatusName(discovered.status);
                if(auto pool=channels_->Find(endpoint)){
                    instance.pool=pool->GetStats();
                }
                auto state=
                    load_balancer_.GetHealthChecker().Find(endpoint);
                if(state!=nullptr){
                    instance.has_health_state=true;
                    instance.health=state->Snapshot();
                }
                result.push_back(std::move(instance));
            }
        }
        return result;
    }

    std::vector<minirpc::gateway::AdminEndpointMetrics>
    Metrics()const{
        std::vector<
            minirpc::gateway::AdminEndpointMetrics
        > result;
        for(const auto& pool:channels_->Snapshot()){
            minirpc::gateway::AdminEndpointMetrics metrics;
            metrics.endpoint=pool->GetEndpoint().ToString();
            metrics.totals=pool->GetMetrics();
            metrics.methods=pool->GetAllMethodMetrics();
            result.push_back(std::move(metrics));
        }
        return result;
    }

    std::vector<minirpc::gateway::AdminServiceConfig>
    Config()const{
        std::vector<minirpc::gateway::AdminServiceConfig> result;
        for(const auto& service:services_){
            result.push_back({
                service.name,
                *config_center_->Get(service.name)
            });
        }
        return result;
    }

    std::vector<minirpc::gateway::AdminTraceInfo>
    Traces()const{
        std::lock_guard<std::mutex> lock(traces_mutex_);
        return {traces_.begin(),traces_.end()};
    }

    minirpc::gateway::AdminHealthInfo Health()const{
        minirpc::gateway::AdminHealthInfo health;
        health.zookeeper_connected=zk_client_->IsConnected();
        health.services=services_.size();
        bool discovery_ready=true;

        for(const auto& service:services_){
            auto discovered=discovery_->Resolve(service.name);
            if(discovered.status!=registry::DiscoveryStatus::Ok||
               discovered.providers->empty()){
                discovery_ready=false;
                continue;
            }

            auto states=
                load_balancer_.GetHealthChecker().Update(
                    discovered.providers
                );
            health.instances+=states->size();
            for(const auto& state:*states){
                if(state->Snapshot().selectable){
                    ++health.selectable_instances;
                }
            }
        }

        health.ready=health.zookeeper_connected&&
                     discovery_ready&&
                     health.selectable_instances!=0;
        return health;
    }

private:
    void ApplyConfig(const config::RpcConfig& config){
        health::EndpointStateOptions endpoint_options;
        endpoint_options.failure_threshold=
            config.failure_threshold;
        endpoint_options.ewma_alpha=config.ewma_alpha;
        load_balancer_.UpdateOptions(endpoint_options);

        health::ActiveHealthCheckOptions active_options;
        active_options.interval=config.health_check_interval;
        active_options.timeout=std::chrono::milliseconds(800);
        load_balancer_.GetHealthChecker().
            UpdateActiveOptions(active_options);
    }

    void RecordTrace(minirpc::gateway::AdminTraceInfo trace){
        constexpr std::size_t max_traces=256;
        std::lock_guard<std::mutex> lock(traces_mutex_);
        traces_.push_front(std::move(trace));
        if(traces_.size()>max_traces){
            traces_.pop_back();
        }
    }

    std::shared_ptr<registry::ZooKeeperClient> zk_client_;
    registry::ZooKeeperDiscovery* discovery_;
    registry::ZooKeeperConfigCenter* config_center_;
    cluster::ChannelManager* channels_;
    log::AsyncLogger* logger_;
    mutable loadbalance::P2cEwmaLoadBalancer load_balancer_;
    cluster::RoundRobin round_robin_;
    std::vector<minirpc::gateway::AdminServiceInfo> services_;
    mutable std::mutex traces_mutex_;
    std::deque<minirpc::gateway::AdminTraceInfo> traces_;
    registry::ZooKeeperConfigCenter::ListenerId config_listener_=0;
    bool started_=false;
};

GatewayRuntime::GatewayRuntime(
    std::shared_ptr<registry::ZooKeeperClient> zk_client,
    registry::ZooKeeperDiscovery* discovery,
    registry::ZooKeeperConfigCenter* config_center,
    cluster::ChannelManager* channels,
    log::AsyncLogger* logger,
    const google::protobuf::ServiceDescriptor* service
):impl_(std::make_shared<Impl>(
      std::move(zk_client),
      discovery,
      config_center,
      channels,
      logger,
      service
  )){}

GatewayRuntime::~GatewayRuntime()=default;

void GatewayRuntime::Start(net::EventLoop* loop){
    impl_->Start(loop);
}

void GatewayRuntime::Stop()noexcept{
    impl_->Stop();
}

void GatewayRuntime::AsyncCall(
    std::string service_name,
    std::string method_name,
    std::string payload,
    rpc::CallOptions options,
    Completion completion
){
    impl_->AsyncCall(
        std::move(service_name),
        std::move(method_name),
        std::move(payload),
        options,
        std::move(completion)
    );
}

std::vector<minirpc::gateway::AdminServiceInfo>
GatewayRuntime::Services()const{
    return impl_->Services();
}

std::vector<minirpc::gateway::AdminInstanceInfo>
GatewayRuntime::Instances()const{
    return impl_->Instances();
}

std::vector<minirpc::gateway::AdminEndpointMetrics>
GatewayRuntime::Metrics()const{
    return impl_->Metrics();
}

std::vector<minirpc::gateway::AdminServiceConfig>
GatewayRuntime::Config()const{
    return impl_->Config();
}

std::vector<minirpc::gateway::AdminTraceInfo>
GatewayRuntime::Traces()const{
    return impl_->Traces();
}

minirpc::gateway::AdminHealthInfo
GatewayRuntime::Health()const{
    return impl_->Health();
}

}
