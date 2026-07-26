#include "minirpc/cluster/ChannelManager.h"
#include "minirpc/cluster/RetryPolicy.h"
#include "minirpc/cluster/RoundRobin.h"
#include "minirpc/config/RpcConfig.h"
#include "minirpc/health/HealthService.h"
#include "minirpc/loadbalance/P2cEwmaLoadBalancer.h"
#include "minirpc/log/AsyncLogger.h"
#include "minirpc/log/LogMacros.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/protocol/RpcMessage.h"
#include "minirpc/registry/ZooKeeperClient.h"
#include "minirpc/registry/ZooKeeperConfigCenter.h"
#include "minirpc/registry/ZooKeeperDiscovery.h"
#include "minirpc/rpc/CallOptions.h"
#include "minirpc/trace/TraceContext.h"

#include <chrono>
#include <cstddef>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

using namespace minirpc;

namespace{

constexpr const char* kServiceName="RegistryDemoService";
constexpr const char* kMethodName="WhoAmI";
using Clock=std::chrono::steady_clock;

bool IsEndpointFailure(protocol::StatusCode status)noexcept{
    return status==protocol::StatusCode::Timeout||
           status==protocol::StatusCode::ConnectionFailed;
}

struct Arguments{
    std::string zookeeper_servers="127.0.0.1:2181";
    std::size_t requests=40;
    std::chrono::milliseconds interval{500};
};

std::size_t ParseSize(const std::string& value,const char* name){
    if(value.empty()||value.front()=='-'){
        throw std::invalid_argument(
            std::string(name)+" must be positive"
        );
    }

    std::size_t parsed=0;
    unsigned long long result=std::stoull(value,&parsed);
    if(parsed!=value.size()||result==0){
        throw std::invalid_argument(
            std::string(name)+" must be positive"
        );
    }
    return static_cast<std::size_t>(result);
}

Arguments ParseArguments(int argc,char* argv[]){
    Arguments arguments;
    if(argc>1){
        arguments.zookeeper_servers=argv[1];
    }
    if(argc>2){
        arguments.requests=ParseSize(argv[2],"requests");
    }
    if(argc>3){
        arguments.interval=std::chrono::milliseconds(
            ParseSize(argv[3],"interval")
        );
    }
    return arguments;
}

class LoopThread{
public:
    LoopThread(){
        std::promise<net::EventLoop*> ready;
        auto future=ready.get_future();

        thread_=std::thread([ready=std::move(ready)]()mutable{
            net::EventLoop loop;
            ready.set_value(&loop);
            loop.Loop();
        });
        loop_=future.get();
    }

    ~LoopThread(){
        loop_->Stop();
        thread_.join();
    }

    LoopThread(const LoopThread&)=delete;
    LoopThread& operator=(const LoopThread&)=delete;

    net::EventLoop* Loop()const noexcept{
        return loop_;
    }

private:
    net::EventLoop* loop_=nullptr;
    std::thread thread_;
};

}

int main(int argc,char* argv[]){
    try{
        Arguments arguments=ParseArguments(argc,argv);

        registry::ZooKeeperClientOptions zk_options;
        zk_options.servers=arguments.zookeeper_servers;
        zk_options.session_timeout=std::chrono::seconds(5);

        auto zk_client=
            std::make_shared<registry::ZooKeeperClient>(zk_options);
        registry::ZooKeeperDiscovery discovery(zk_client);
        discovery.SetErrorCallback([](const std::string& error){
            std::cerr<<"discovery error: "<<error<<'\n';
        });

        zk_client->Start();
        if(!zk_client->WaitUntilConnected(std::chrono::seconds(10))){
            throw std::runtime_error("ZooKeeper connect timeout");
        }
        discovery.WatchService(kServiceName);

        LoopThread loop_thread;
        cluster::ChannelManager channels(loop_thread.Loop());
        cluster::RoundRobin round_robin;
        loadbalance::P2cEwmaLoadBalancer load_balancer;
        log::LoggerOptions logger_options;
        logger_options.file_path="logs/registry_client.log";
        log::AsyncLogger logger(logger_options);

        // 配置回调会引用load_balancer，因此配置中心后构造、先析构。
        registry::ZooKeeperConfigCenter config_center(zk_client);
        config_center.SetErrorCallback([](const std::string& error){
            std::cerr<<"config error: "<<error<<'\n';
        });
        config_center.WatchService(kServiceName);

        health::ActiveHealthCheckOptions health_options;
        health_options.interval=std::chrono::seconds(2);
        health_options.timeout=std::chrono::milliseconds(800);

        load_balancer.GetHealthChecker().Start(
            loop_thread.Loop(),
            [&channels](
                const cluster::Endpoint& endpoint,
                std::chrono::milliseconds timeout,
                health::HealthChecker::ProbeCompletion completion
            ){
                rpc::CallOptions options;
                options.idempotent=true;
                options.timeout=timeout;

                try{
                    auto pool=channels.GetOrCreate(endpoint);
                    pool->AsyncCall(
                        health::HealthService::ServiceName(),
                        health::HealthService::MethodName(),
                        {},
                        [completion](
                            protocol::RpcMessage response
                        ){
                            bool serving=
                                response.meta.status_code==
                                    protocol::StatusCode::Ok&&
                                response.payload==
                                    health::HealthService::
                                        ServingPayload();
                            completion(serving);
                        },
                        options
                    );
                }catch(...){
                    completion(false);
                }
            },
            health_options
        );

        auto config_listener=config_center.Subscribe(
            kServiceName,
            [&load_balancer,health_options](
                config::ConfigStore::Snapshot snapshot
            )mutable{
                health::EndpointStateOptions endpoint_options;
                endpoint_options.failure_threshold=
                    snapshot->failure_threshold;
                endpoint_options.ewma_alpha=snapshot->ewma_alpha;
                load_balancer.UpdateOptions(endpoint_options);

                health_options.interval=
                    snapshot->health_check_interval;
                load_balancer.GetHealthChecker().
                    UpdateActiveOptions(health_options);
            }
        );

        for(std::size_t request=1;
            request<=arguments.requests;
            ++request){
            registry::DiscoveryResult discovered=
                discovery.Resolve(kServiceName);
            auto runtime_config=config_center.Get(kServiceName);

            std::optional<
                loadbalance::P2cEwmaLoadBalancer::Selection
            > selection;
            std::optional<cluster::Endpoint> selected_endpoint;

            if(runtime_config->load_balancer==
               config::LoadBalancerAlgorithm::RoundRobin){
                selected_endpoint=round_robin.Select(
                    discovered.providers
                );
            }else{
                selection=load_balancer.Select(
                    discovered.providers
                );
                if(selection){
                    selected_endpoint=selection->GetEndpoint();
                }
            }

            if(!selected_endpoint.has_value()){
                const char* state=
                    discovered.status==
                        registry::DiscoveryStatus::NoProvider
                    ?"NoProvider":"NotReady";
                std::cout<<"request="<<request
                         <<" discovery="<<state
                         <<std::endl;
            }else{
                cluster::Endpoint endpoint=*selected_endpoint;
                trace::TraceContext root_trace=
                    trace::CreateRootTrace();
                trace::TraceScope trace_scope(root_trace);
                MINIRPC_LOG_INFO(
                    logger,
                    "calling "+std::string(kServiceName)+'.'+
                    kMethodName+" endpoint="+endpoint.ToString()
                );
                auto pool=channels.GetOrCreate(endpoint);
                auto started_at=Clock::now();
                protocol::RpcMessage response;
                rpc::CallOptions call_options;
                call_options.idempotent=true;
                call_options.timeout=
                    runtime_config->default_timeout;
                cluster::RetryPolicy retry_policy(
                    static_cast<std::size_t>(
                        runtime_config->retry_count
                    )+1,
                    std::chrono::milliseconds(10)
                );

                try{
                    response=pool->Call(
                        kServiceName,
                        kMethodName,
                        {},
                        call_options,
                        retry_policy
                    );
                }catch(...){
                    auto latency=
                        std::chrono::duration_cast<
                            std::chrono::microseconds
                        >(Clock::now()-started_at);
                    if(selection){
                        selection->CompleteFailure(latency);
                    }
                    throw;
                }

                auto latency=
                    std::chrono::duration_cast<
                        std::chrono::microseconds
                    >(Clock::now()-started_at);
                if(selection){
                    if(IsEndpointFailure(response.meta.status_code)){
                        selection->CompleteFailure(latency);
                    }else{
                        selection->CompleteSuccess(latency);
                    }
                }

                MINIRPC_LOG_INFO(
                    logger,
                    "completed "+std::string(kServiceName)+'.'+
                    kMethodName+" endpoint="+endpoint.ToString()+
                    " status="+std::to_string(
                        static_cast<int>(
                            response.meta.status_code
                        )
                    )
                );

                std::cout<<"request="<<request
                         <<" selected="<<endpoint.ToString()
                         <<" providers="
                         <<discovered.providers->size()
                         <<" lb="
                         <<config::ToString(
                             runtime_config->load_balancer
                         );

                if(response.meta.status_code==
                   protocol::StatusCode::Ok){
                    std::cout<<" response="<<response.payload;
                }else{
                    std::cout<<" error="
                             <<response.meta.error_text;
                }
                std::cout<<std::endl;
            }

            std::this_thread::sleep_for(arguments.interval);
        }

        config_center.Unsubscribe(config_listener);
        zk_client->Close();
        load_balancer.GetHealthChecker().Stop();
        logger.Stop();
        return 0;
    }catch(const std::exception& error){
        std::cerr<<"registry client error: "
                 <<error.what()<<'\n';
        return 1;
    }
}
