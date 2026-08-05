#include "asterrpc/registry/ZooKeeperDiscovery.h"

#include "asterrpc/registry/ZooKeeperClient.h"
#include "asterrpc/registry/ZooKeeperProvider.h"

#include <zookeeper/zookeeper.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace asterrpc::registry{
namespace{

std::optional<cluster::Endpoint> ParseEndpoint(
    const std::string& value
){
    std::size_t separator=value.rfind(':');
    if(separator==std::string::npos||
       separator==0||
       separator+1>=value.size()){
        return std::nullopt;
    }

    std::size_t parsed=0;
    unsigned long port=0;
    try{
        port=std::stoul(value.substr(separator+1),&parsed);
    }catch(const std::exception&){
        return std::nullopt;
    }

    if(parsed!=value.size()-separator-1||
       port==0||
       port>65535){
        return std::nullopt;
    }

    try{
        return cluster::Endpoint(
            value.substr(0,separator),
            static_cast<std::uint16_t>(port)
        );
    }catch(const std::invalid_argument&){
        return std::nullopt;
    }
}

ProviderSnapshot EmptySnapshot(){
    static ProviderSnapshot empty=
        std::make_shared<const ProviderList>();
    return empty;
}

}

class ZooKeeperDiscovery::State:
    public std::enable_shared_from_this<ZooKeeperDiscovery::State>{
public:
    State(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path
    ):client_(std::move(client)),
      root_path_(std::move(root_path)){
        if(client_==nullptr){
            throw std::invalid_argument(
                "ZooKeeper discovery client is null"
            );
        }

        ZooKeeperProvider::ProviderParentPath(
            root_path_,
            "ValidationService"
        );
    }

    ~State(){
        Stop();
    }

    void Start(){
        std::weak_ptr<State> weak_self=shared_from_this();
        listener_id_=client_->AddStateListener(
            [weak_self](const ZooKeeperConnectionEvent& event){
                if(auto self=weak_self.lock()){
                    self->HandleState(event);
                }
            }
        );
    }

    void Stop()noexcept{
        ZooKeeperClient::ListenerId listener=0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            listener=listener_id_;
            listener_id_=0;
        }

        if(listener!=0){
            client_->RemoveStateListener(listener);
        }
    }

    void WatchService(const std::string& service_name){
        std::string parent=
            ZooKeeperProvider::ProviderParentPath(
                root_path_,
                service_name
            );

        std::shared_ptr<ServiceEntry> entry;
        bool inserted=false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto result=services_.emplace(
                service_name,
                std::make_shared<ServiceEntry>(
                    service_name,
                    std::move(parent)
                )
            );
            entry=result.first->second;
            inserted=result.second;
        }

        if(!inserted){
            return;
        }

        ZooKeeperConnectionEvent current=client_->CurrentEvent();
        if(current.state==ZooKeeperConnectionState::Connected&&
           entry->watched_generation.load(
               std::memory_order_acquire
           )!=current.generation){
            Refresh(entry,true);
        }
    }

    DiscoveryResult Resolve(
        const std::string& service_name
    )const{
        std::shared_ptr<ServiceEntry> entry;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto service=services_.find(service_name);
            if(service==services_.end()){
                return {
                    DiscoveryStatus::NotReady,
                    EmptySnapshot()
                };
            }
            entry=service->second;
        }

        ProviderSnapshot snapshot=std::atomic_load_explicit(
            &entry->snapshot,
            std::memory_order_acquire
        );
        if(!entry->initialized.load(std::memory_order_acquire)){
            return {
                DiscoveryStatus::NotReady,
                std::move(snapshot)
            };
        }
        if(snapshot->empty()){
            return {
                DiscoveryStatus::NoProvider,
                std::move(snapshot)
            };
        }
        return {
            DiscoveryStatus::Ok,
            std::move(snapshot)
        };
    }

    void SetErrorCallback(ErrorCallback callback){
        std::lock_guard<std::mutex> lock(mutex_);
        error_callback_=std::move(callback);
    }

private:
    struct ServiceEntry{
        ServiceEntry(
            std::string service_name_value,
            std::string parent_path_value
        ):service_name(std::move(service_name_value)),
          parent_path(std::move(parent_path_value)),
          snapshot(EmptySnapshot()){}

        std::string service_name;
        std::string parent_path;
        std::shared_ptr<const ProviderList> snapshot;
        std::atomic_bool initialized{false};
        std::atomic_uint64_t watched_generation{0};
        std::mutex refresh_mutex;
    };

    void HandleState(const ZooKeeperConnectionEvent& event){
        if(event.state!=ZooKeeperConnectionState::Connected){
            return;
        }

        std::vector<std::shared_ptr<ServiceEntry>> entries;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            entries.reserve(services_.size());
            for(const auto& service:services_){
                entries.push_back(service.second);
            }
        }

        for(const auto& entry:entries){
            if(entry->watched_generation.load(
                   std::memory_order_acquire
               )!=event.generation){
                Refresh(entry,false);
            }
        }
    }

    void Refresh(
        const std::shared_ptr<ServiceEntry>& entry,
        bool rethrow,
        bool watch_triggered=false
    ){
        std::lock_guard<std::mutex> refresh_lock(
            entry->refresh_mutex
        );

        try{
            if(watch_triggered){
                entry->watched_generation.store(
                    0,
                    std::memory_order_release
                );
            }

            ZooKeeperConnectionEvent watch_event=
                client_->CurrentEvent();
            if(watch_event.state!=
               ZooKeeperConnectionState::Connected){
                return;
            }
            if(entry->watched_generation.load(
                   std::memory_order_acquire
               )==watch_event.generation){
                // 同一个Session中的Watch仍然有效，不重复注册。
                return;
            }

            client_->EnsurePersistentPath(entry->parent_path);

            std::weak_ptr<State> weak_self=shared_from_this();
            std::weak_ptr<ServiceEntry> weak_entry=entry;
            std::vector<std::string> children=
                client_->GetChildrenAndWatch(
                    entry->parent_path,
                    [weak_self,weak_entry](){
                        auto self=weak_self.lock();
                        auto watched_entry=weak_entry.lock();
                        if(self!=nullptr&&watched_entry!=nullptr){
                            self->Refresh(
                                watched_entry,
                                false,
                                true
                            );
                        }
                    }
                );

            ProviderList providers;
            providers.reserve(children.size());

            for(const std::string& child:children){
                try{
                    std::string data=client_->GetData(
                        entry->parent_path+'/'+child
                    );
                    std::optional<cluster::Endpoint> endpoint=
                        ParseEndpoint(data);
                    if(endpoint.has_value()){
                        providers.push_back(std::move(*endpoint));
                    }else{
                        ReportError(
                            "invalid provider endpoint in "+
                            entry->parent_path+'/'+child
                        );
                    }
                }catch(const ZooKeeperError& error){
                    if(error.Code()!=ZNONODE){
                        throw;
                    }
                }
            }

            std::sort(
                providers.begin(),
                providers.end(),
                [](const cluster::Endpoint& left,
                   const cluster::Endpoint& right){
                    return left.ToString()<right.ToString();
                }
            );
            providers.erase(
                std::unique(providers.begin(),providers.end()),
                providers.end()
            );

            ZooKeeperConnectionEvent current=
                client_->CurrentEvent();
            if(current.state!=
                   ZooKeeperConnectionState::Connected||
               current.generation!=watch_event.generation){
                return;
            }

            ProviderSnapshot snapshot=
                std::make_shared<const ProviderList>(
                    std::move(providers)
                );
            // 发布完整的新快照，读线程不会看到构造中的ProviderList。
            std::atomic_store_explicit(
                &entry->snapshot,
                std::move(snapshot),
                std::memory_order_release
            );
            entry->initialized.store(true,std::memory_order_release);
            entry->watched_generation.store(
                watch_event.generation,
                std::memory_order_release
            );
        }catch(const std::exception& error){
            ReportError(error.what());
            if(rethrow){
                throw;
            }
        }
    }

    void ReportError(const std::string& error){
        ErrorCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback=error_callback_;
        }

        if(callback){
            try{
                callback(error);
            }catch(...){
            }
        }
    }

    std::shared_ptr<ZooKeeperClient> client_;
    std::string root_path_;

    mutable std::mutex mutex_;
    ZooKeeperClient::ListenerId listener_id_=0;
    std::map<std::string,std::shared_ptr<ServiceEntry>> services_;
    ErrorCallback error_callback_;
};

ZooKeeperDiscovery::ZooKeeperDiscovery(
    std::shared_ptr<ZooKeeperClient> client,
    std::string root_path
):state_(std::make_shared<State>(
      std::move(client),
      std::move(root_path)
  )){
    state_->Start();
}

ZooKeeperDiscovery::~ZooKeeperDiscovery()=default;

void ZooKeeperDiscovery::WatchService(
    const std::string& service_name
){
    state_->WatchService(service_name);
}

DiscoveryResult ZooKeeperDiscovery::Resolve(
    const std::string& service_name
)const{
    return state_->Resolve(service_name);
}

void ZooKeeperDiscovery::SetErrorCallback(ErrorCallback callback){
    state_->SetErrorCallback(std::move(callback));
}

}
