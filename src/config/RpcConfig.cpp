#include "asterrpc/config/RpcConfig.h"

#include <nlohmann/json.hpp>

#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace asterrpc::config{
namespace{

using Json=nlohmann::json;

void ValidateServiceName(const std::string& service_name){
    if(service_name.empty()||
       service_name.find('/')!=std::string::npos){
        throw std::invalid_argument("invalid RPC service name");
    }
}

template<class Integer>
Integer ReadUnsigned(
    const Json& value,
    const char* field,
    std::uint64_t maximum
){
    if(!value.is_number_unsigned()&&!value.is_number_integer()){
        throw std::invalid_argument(
            std::string(field)+" must be an integer"
        );
    }

    std::int64_t signed_value=0;
    if(value.is_number_unsigned()){
        std::uint64_t unsigned_value=value.get<std::uint64_t>();
        if(unsigned_value>maximum){
            throw std::invalid_argument(
                std::string(field)+" is too large"
            );
        }
        return static_cast<Integer>(unsigned_value);
    }

    signed_value=value.get<std::int64_t>();
    if(signed_value<0||
       static_cast<std::uint64_t>(signed_value)>maximum){
        throw std::invalid_argument(
            std::string(field)+" is out of range"
        );
    }
    return static_cast<Integer>(signed_value);
}

std::chrono::milliseconds ReadMilliseconds(
    const Json& value,
    const char* field,
    bool allow_zero
){
    auto count=ReadUnsigned<std::uint64_t>(
        value,
        field,
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::chrono::milliseconds::rep
            >::max()
        )
    );
    if(!allow_zero&&count==0){
        throw std::invalid_argument(
            std::string(field)+" must be positive"
        );
    }
    return std::chrono::milliseconds(count);
}

LoadBalancerAlgorithm ParseLoadBalancer(const Json& value){
    if(!value.is_string()){
        throw std::invalid_argument(
            "load_balancer must be a string"
        );
    }

    std::string algorithm=value.get<std::string>();
    if(algorithm=="round_robin"){
        return LoadBalancerAlgorithm::RoundRobin;
    }
    if(algorithm=="p2c_ewma"){
        return LoadBalancerAlgorithm::P2cEwma;
    }

    throw std::invalid_argument(
        "load_balancer must be round_robin or p2c_ewma"
    );
}

}

const char* ToString(LoadBalancerAlgorithm algorithm)noexcept{
    switch(algorithm){
        case LoadBalancerAlgorithm::RoundRobin:
            return "round_robin";
        case LoadBalancerAlgorithm::P2cEwma:
            return "p2c_ewma";
    }
    return "unknown";
}

void Validate(const RpcConfig& config){
    if(config.default_timeout<std::chrono::milliseconds::zero()){
        throw std::invalid_argument(
            "default timeout must not be negative"
        );
    }
    if(config.health_check_interval<=
       std::chrono::milliseconds::zero()){
        throw std::invalid_argument(
            "health check interval must be positive"
        );
    }
    if(config.failure_threshold==0){
        throw std::invalid_argument(
            "failure threshold must be positive"
        );
    }
    if(!std::isfinite(config.ewma_alpha)||
       config.ewma_alpha<=0.0||
       config.ewma_alpha>1.0){
        throw std::invalid_argument(
            "EWMA alpha must be in the range (0, 1]"
        );
    }
}

RpcConfigPatch ParseRpcConfigPatch(std::string_view json){
    if(json.empty()){
        return {};
    }

    Json document;
    try{
        document=Json::parse(json.begin(),json.end());
    }catch(const Json::exception& error){
        throw std::invalid_argument(
            "invalid RPC config JSON: "+
            std::string(error.what())
        );
    }

    if(!document.is_object()){
        throw std::invalid_argument(
            "RPC config JSON must be an object"
        );
    }

    RpcConfigPatch patch;
    for(const auto& item:document.items()){
        const std::string& key=item.key();
        const Json& value=item.value();

        if(key=="default_timeout_ms"){
            patch.default_timeout=ReadMilliseconds(
                value,
                "default_timeout_ms",
                true
            );
        }else if(key=="retry_count"){
            patch.retry_count=ReadUnsigned<std::uint32_t>(
                value,
                "retry_count",
                std::numeric_limits<std::uint32_t>::max()
            );
        }else if(key=="load_balancer"){
            patch.load_balancer=ParseLoadBalancer(value);
        }else if(key=="health_check_interval_ms"){
            patch.health_check_interval=ReadMilliseconds(
                value,
                "health_check_interval_ms",
                false
            );
        }else if(key=="failure_threshold"){
            patch.failure_threshold=ReadUnsigned<std::size_t>(
                value,
                "failure_threshold",
                std::numeric_limits<std::size_t>::max()
            );
            if(*patch.failure_threshold==0){
                throw std::invalid_argument(
                    "failure_threshold must be positive"
                );
            }
        }else if(key=="ewma_alpha"){
            if(!value.is_number()){
                throw std::invalid_argument(
                    "ewma_alpha must be a number"
                );
            }
            patch.ewma_alpha=value.get<double>();
            if(!std::isfinite(*patch.ewma_alpha)||
               *patch.ewma_alpha<=0.0||
               *patch.ewma_alpha>1.0){
                throw std::invalid_argument(
                    "ewma_alpha must be in the range (0, 1]"
                );
            }
        }else{
            throw std::invalid_argument(
                "unknown RPC config field: "+key
            );
        }
    }

    return patch;
}

RpcConfig Merge(
    const RpcConfig& base,
    const RpcConfigPatch& patch
){
    RpcConfig result=base;
    if(patch.default_timeout){
        result.default_timeout=*patch.default_timeout;
    }
    if(patch.retry_count){
        result.retry_count=*patch.retry_count;
    }
    if(patch.load_balancer){
        result.load_balancer=*patch.load_balancer;
    }
    if(patch.health_check_interval){
        result.health_check_interval=
            *patch.health_check_interval;
    }
    if(patch.failure_threshold){
        result.failure_threshold=*patch.failure_threshold;
    }
    if(patch.ewma_alpha){
        result.ewma_alpha=*patch.ewma_alpha;
    }

    Validate(result);
    return result;
}

class ConfigStore::Impl{
public:
    explicit Impl(RpcConfig defaults)
    :defaults_(std::move(defaults)){
        Validate(defaults_);
        global_snapshot_=std::make_shared<const RpcConfig>(defaults_);
    }

    Snapshot Get(const std::string& service_name)const{
        std::lock_guard<std::mutex> lock(mutex_);
        auto service=services_.find(service_name);
        return service==services_.end()
            ?global_snapshot_
            :service->second.snapshot;
    }

    void UpdateGlobal(RpcConfigPatch patch){
        std::vector<Notification> notifications;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            RpcConfig merged=Merge(defaults_,patch);
            global_snapshot_=
                std::make_shared<const RpcConfig>(std::move(merged));

            AppendNotifications("",global_snapshot_,&notifications);
            for(auto& service:services_){
                PublishServiceInLock(
                    service.first,
                    &service.second,
                    &notifications
                );
            }
        }
        Notify(std::move(notifications));
    }

    void UpdateService(
        std::string service_name,
        RpcConfigPatch patch
    ){
        ValidateServiceName(service_name);
        std::vector<Notification> notifications;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ServiceEntry& service=services_[service_name];
            service.patch=std::move(patch);
            PublishServiceInLock(
                service_name,
                &service,
                &notifications
            );
        }
        Notify(std::move(notifications));
    }

    ListenerId Subscribe(
        std::string service_name,
        UpdateCallback callback
    ){
        ValidateServiceName(service_name);
        if(!callback){
            throw std::invalid_argument(
                "config update callback is empty"
            );
        }

        ListenerId id=0;
        Snapshot snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            id=next_listener_id_++;
            while(id==0){
                id=next_listener_id_++;
            }

            listeners_.emplace(
                id,
                Listener{std::move(service_name),callback}
            );
            const Listener& listener=listeners_.at(id);
            auto service=services_.find(listener.service_name);
            snapshot=service==services_.end()
                ?global_snapshot_
                :service->second.snapshot;
        }

        try{
            callback(std::move(snapshot));
        }catch(...){
        }
        return id;
    }

    void Unsubscribe(ListenerId id)noexcept{
        std::lock_guard<std::mutex> lock(mutex_);
        listeners_.erase(id);
    }

private:
    struct ServiceEntry{
        RpcConfigPatch patch;
        Snapshot snapshot;
    };

    struct Listener{
        std::string service_name;
        UpdateCallback callback;
    };

    struct Notification{
        UpdateCallback callback;
        Snapshot snapshot;
    };

    void PublishServiceInLock(
        const std::string& service_name,
        ServiceEntry* service,
        std::vector<Notification>* notifications
    ){
        service->snapshot=std::make_shared<const RpcConfig>(
            Merge(*global_snapshot_,service->patch)
        );
        AppendNotifications(
            service_name,
            service->snapshot,
            notifications
        );
    }

    void AppendNotifications(
        const std::string& service_name,
        const Snapshot& snapshot,
        std::vector<Notification>* notifications
    )const{
        for(const auto& listener:listeners_){
            if(service_name.empty()||
               listener.second.service_name==service_name){
                Snapshot effective=snapshot;
                if(service_name.empty()){
                    auto service=services_.find(
                        listener.second.service_name
                    );
                    if(service!=services_.end()){
                        continue;
                    }
                }
                notifications->push_back({
                    listener.second.callback,
                    std::move(effective)
                });
            }
        }
    }

    static void Notify(
        std::vector<Notification> notifications
    )noexcept{
        for(auto& notification:notifications){
            try{
                notification.callback(
                    std::move(notification.snapshot)
                );
            }catch(...){
            }
        }
    }

    RpcConfig defaults_;

    mutable std::mutex mutex_;
    Snapshot global_snapshot_;
    std::map<std::string,ServiceEntry> services_;
    std::map<ListenerId,Listener> listeners_;
    ListenerId next_listener_id_=1;
};

ConfigStore::ConfigStore(RpcConfig defaults)
:impl_(std::make_unique<Impl>(std::move(defaults))){}

ConfigStore::~ConfigStore()=default;

ConfigStore::Snapshot ConfigStore::Get(
    const std::string& service_name
)const{
    return impl_->Get(service_name);
}

void ConfigStore::UpdateGlobal(RpcConfigPatch patch){
    impl_->UpdateGlobal(std::move(patch));
}

void ConfigStore::UpdateService(
    std::string service_name,
    RpcConfigPatch patch
){
    impl_->UpdateService(
        std::move(service_name),
        std::move(patch)
    );
}

ConfigStore::ListenerId ConfigStore::Subscribe(
    std::string service_name,
    UpdateCallback callback
){
    return impl_->Subscribe(
        std::move(service_name),
        std::move(callback)
    );
}

void ConfigStore::Unsubscribe(ListenerId id)noexcept{
    impl_->Unsubscribe(id);
}

}
