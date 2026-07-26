#include "minirpc/registry/ZooKeeperConfigCenter.h"

#include "minirpc/registry/ZooKeeperClient.h"

#include <atomic>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace minirpc::registry{
namespace{

std::string NormalizeRoot(std::string root_path){
    if(root_path.empty()||root_path.front()!='/'){
        throw std::invalid_argument(
            "ZooKeeper config root must be absolute"
        );
    }
    if(root_path.find("//")!=std::string::npos){
        throw std::invalid_argument(
            "ZooKeeper config root contains an empty segment"
        );
    }
    while(root_path.size()>1&&root_path.back()=='/'){
        root_path.pop_back();
    }
    return root_path;
}

void ValidateServiceName(const std::string& service_name){
    if(service_name.empty()||
       service_name.find('/')!=std::string::npos){
        throw std::invalid_argument("invalid RPC service name");
    }
}

}

class ZooKeeperConfigCenter::State:
    public std::enable_shared_from_this<State>{
public:
    State(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path,
        config::RpcConfig defaults
    ):client_(std::move(client)),
      root_path_(NormalizeRoot(std::move(root_path))),
      store_(std::move(defaults)),
      global_(std::make_shared<Entry>(
          "",
          ZooKeeperConfigCenter::GlobalPath(root_path_)
      )){
        if(client_==nullptr){
            throw std::invalid_argument(
                "ZooKeeper config client is null"
            );
        }
    }

    ~State(){
        Stop();
    }

    void Start(){
        std::weak_ptr<State> weak=shared_from_this();
        listener_id_=client_->AddStateListener(
            [weak](const ZooKeeperConnectionEvent& event){
                if(auto self=weak.lock()){
                    self->HandleState(event);
                }
            }
        );

        ZooKeeperConnectionEvent current=client_->CurrentEvent();
        if(current.state==ZooKeeperConnectionState::Connected){
            HandleState(current);
        }
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
        ValidateServiceName(service_name);

        std::shared_ptr<Entry> entry;
        bool inserted=false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto result=services_.emplace(
                service_name,
                std::make_shared<Entry>(
                    service_name,
                    ZooKeeperConfigCenter::ServicePath(
                        root_path_,
                        service_name
                    )
                )
            );
            entry=result.first->second;
            inserted=result.second;
        }
        if(!inserted){
            return;
        }

        ZooKeeperConnectionEvent current=client_->CurrentEvent();
        if(current.state==ZooKeeperConnectionState::Connected){
            if(global_->watched_generation.load(
                   std::memory_order_acquire
               )!=current.generation){
                Refresh(global_,true);
            }
            Refresh(entry,true);
        }
    }

    Snapshot Get(const std::string& service_name)const{
        return store_.Get(service_name);
    }

    ListenerId Subscribe(
        std::string service_name,
        UpdateCallback callback
    ){
        return store_.Subscribe(
            std::move(service_name),
            std::move(callback)
        );
    }

    void Unsubscribe(ListenerId listener_id)noexcept{
        store_.Unsubscribe(listener_id);
    }

    void SetErrorCallback(ErrorCallback callback){
        std::lock_guard<std::mutex> lock(mutex_);
        error_callback_=std::move(callback);
    }

private:
    struct Entry{
        Entry(
            std::string service_name_value,
            std::string path_value
        ):service_name(std::move(service_name_value)),
          path(std::move(path_value)){}

        bool IsGlobal()const noexcept{
            return service_name.empty();
        }

        std::string service_name;
        std::string path;
        std::atomic_uint64_t watched_generation{0};
        std::mutex refresh_mutex;
    };

    void HandleState(const ZooKeeperConnectionEvent& event){
        if(event.state!=ZooKeeperConnectionState::Connected){
            return;
        }

        if(global_->watched_generation.load(
               std::memory_order_acquire
           )!=event.generation){
            Refresh(global_,false);
        }

        std::vector<std::shared_ptr<Entry>> entries;
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
        const std::shared_ptr<Entry>& entry,
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
                return;
            }

            client_->EnsurePersistentPath(entry->path);
            std::weak_ptr<State> weak_self=shared_from_this();
            std::weak_ptr<Entry> weak_entry=entry;
            std::string data=client_->GetDataAndWatch(
                entry->path,
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

            ZooKeeperConnectionEvent current=
                client_->CurrentEvent();
            if(current.state!=
                   ZooKeeperConnectionState::Connected||
               current.generation!=watch_event.generation){
                return;
            }

            try{
                config::RpcConfigPatch patch=
                    config::ParseRpcConfigPatch(data);
                if(entry->IsGlobal()){
                    store_.UpdateGlobal(std::move(patch));
                }else{
                    store_.UpdateService(
                        entry->service_name,
                        std::move(patch)
                    );
                }
            }catch(const std::exception& error){
                ReportError(
                    "invalid config in "+entry->path+": "+
                    error.what()
                );
            }

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
    config::ConfigStore store_;
    std::shared_ptr<Entry> global_;

    mutable std::mutex mutex_;
    ZooKeeperClient::ListenerId listener_id_=0;
    std::map<std::string,std::shared_ptr<Entry>> services_;
    ErrorCallback error_callback_;
};

ZooKeeperConfigCenter::ZooKeeperConfigCenter(
    std::shared_ptr<ZooKeeperClient> client,
    std::string root_path,
    config::RpcConfig defaults
):state_(std::make_shared<State>(
      std::move(client),
      std::move(root_path),
      std::move(defaults)
  )){
    state_->Start();
}

ZooKeeperConfigCenter::~ZooKeeperConfigCenter()=default;

void ZooKeeperConfigCenter::WatchService(
    const std::string& service_name
){
    state_->WatchService(service_name);
}

ZooKeeperConfigCenter::Snapshot ZooKeeperConfigCenter::Get(
    const std::string& service_name
)const{
    return state_->Get(service_name);
}

ZooKeeperConfigCenter::ListenerId
ZooKeeperConfigCenter::Subscribe(
    std::string service_name,
    UpdateCallback callback
){
    return state_->Subscribe(
        std::move(service_name),
        std::move(callback)
    );
}

void ZooKeeperConfigCenter::Unsubscribe(
    ListenerId listener_id
)noexcept{
    state_->Unsubscribe(listener_id);
}

void ZooKeeperConfigCenter::SetErrorCallback(
    ErrorCallback callback
){
    state_->SetErrorCallback(std::move(callback));
}

std::string ZooKeeperConfigCenter::GlobalPath(
    const std::string& root_path
){
    return NormalizeRoot(root_path)+"/global";
}

std::string ZooKeeperConfigCenter::ServicePath(
    const std::string& root_path,
    const std::string& service_name
){
    ValidateServiceName(service_name);
    return NormalizeRoot(root_path)+'/'+service_name;
}

}
