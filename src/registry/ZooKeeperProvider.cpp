#include "asterrpc/registry/ZooKeeperProvider.h"

#include "asterrpc/registry/ZooKeeperClient.h"

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace asterrpc::registry{
namespace{

void ValidateServiceName(const std::string& service_name){
    if(service_name.empty()){
        throw std::invalid_argument(
            "ZooKeeper service name must not be empty"
        );
    }
    if(service_name.find('/')!=std::string::npos){
        throw std::invalid_argument(
            "ZooKeeper service name must not contain '/'"
        );
    }
}

std::string NormalizeRootPath(std::string root_path){
    if(root_path.empty()||root_path.front()!='/'){
        throw std::invalid_argument(
            "ZooKeeper provider root path must be absolute"
        );
    }
    if(root_path=="/"){
        throw std::invalid_argument(
            "ZooKeeper provider root path must not be '/'"
        );
    }
    if(root_path.size()>1&&root_path.back()=='/'){
        root_path.pop_back();
    }
    if(root_path.find("//")!=std::string::npos){
        throw std::invalid_argument(
            "ZooKeeper provider root path contains an empty segment"
        );
    }
    return root_path;
}

}

class ZooKeeperProvider::State:
    public std::enable_shared_from_this<ZooKeeperProvider::State>{
public:
    State(
        std::shared_ptr<ZooKeeperClient> client,
        std::string root_path
    ):client_(std::move(client)),
      root_path_(NormalizeRootPath(std::move(root_path))){
        if(client_==nullptr){
            throw std::invalid_argument(
                "ZooKeeper provider client is null"
            );
        }
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

    void Register(
        std::string service_name,
        cluster::Endpoint endpoint
    ){
        ValidateServiceName(service_name);

        std::uint64_t id=0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto existing=std::find_if(
                registrations_.begin(),
                registrations_.end(),
                [&](const Registration& registration){
                    return registration.service_name==service_name&&
                           registration.endpoint==endpoint;
                }
            );
            if(existing!=registrations_.end()){
                id=existing->id;
            }else{
                id=next_registration_id_++;
                while(id==0){
                    id=next_registration_id_++;
                }
                registrations_.push_back({
                    id,
                    std::move(service_name),
                    std::move(endpoint),
                    {},
                    0
                });
            }
        }

        ZooKeeperConnectionEvent event=client_->CurrentEvent();
        if(event.state==ZooKeeperConnectionState::Connected){
            RegisterOne(id,event,true);
        }
    }

    std::string RegisteredNode(
        const std::string& service_name,
        const cluster::Endpoint& endpoint
    )const{
        std::lock_guard<std::mutex> lock(mutex_);
        auto registration=std::find_if(
            registrations_.begin(),
            registrations_.end(),
            [&](const Registration& value){
                return value.service_name==service_name&&
                       value.endpoint==endpoint;
            }
        );
        return registration==registrations_.end()
            ?std::string{}
            :registration->node_path;
    }

    void SetErrorCallback(ErrorCallback callback){
        std::lock_guard<std::mutex> lock(mutex_);
        error_callback_=std::move(callback);
    }

private:
    struct Registration{
        std::uint64_t id;
        std::string service_name;
        cluster::Endpoint endpoint;
        std::string node_path;
        std::uint64_t generation;
        std::uint64_t registering_generation=0;
    };

    void HandleState(const ZooKeeperConnectionEvent& event){
        if(event.state!=ZooKeeperConnectionState::Connected){
            return;
        }

        std::vector<std::uint64_t> pending;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for(const Registration& registration:registrations_){
                if(registration.generation!=event.generation||
                   registration.node_path.empty()){
                    pending.push_back(registration.id);
                }
            }
        }

        for(std::uint64_t id:pending){
            RegisterOne(id,event,false);
        }
    }

    void RegisterOne(
        std::uint64_t id,
        const ZooKeeperConnectionEvent& event,
        bool rethrow
    ){
        std::string service_name;
        std::string endpoint_data;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto registration=FindRegistration(id);
            if(registration==registrations_.end()){
                return;
            }
            if(registration->generation==event.generation&&
               !registration->node_path.empty()){
                return;
            }
            if(registration->registering_generation==event.generation){
                return;
            }

            service_name=registration->service_name;
            endpoint_data=registration->endpoint.ToString();
            registration->registering_generation=event.generation;
        }

        try{
            std::string parent=ZooKeeperProvider::ProviderParentPath(
                root_path_,
                service_name
            );
            client_->EnsurePersistentPath(parent);
            std::string node_path=
                client_->CreateEphemeralSequential(
                    parent+"/instance-",
                    endpoint_data
                );

            ZooKeeperConnectionEvent current=client_->CurrentEvent();
            if(current.state!=ZooKeeperConnectionState::Connected||
               current.generation!=event.generation){
                ClearRegisteringGeneration(id,event.generation);
                return;
            }

            std::lock_guard<std::mutex> lock(mutex_);
            auto registration=FindRegistration(id);
            if(registration!=registrations_.end()){
                registration->node_path=std::move(node_path);
                registration->generation=event.generation;
                registration->registering_generation=0;
            }
        }catch(const std::exception& error){
            ClearRegisteringGeneration(id,event.generation);
            ReportError(error.what());
            if(rethrow){
                throw;
            }
        }
    }

    std::vector<Registration>::iterator FindRegistration(
        std::uint64_t id
    ){
        return std::find_if(
            registrations_.begin(),
            registrations_.end(),
            [id](const Registration& registration){
                return registration.id==id;
            }
        );
    }

    void ClearRegisteringGeneration(
        std::uint64_t id,
        std::uint64_t generation
    ){
        std::lock_guard<std::mutex> lock(mutex_);
        auto registration=FindRegistration(id);
        if(registration!=registrations_.end()&&
           registration->registering_generation==generation){
            registration->registering_generation=0;
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
    std::uint64_t next_registration_id_=1;
    std::vector<Registration> registrations_;
    ErrorCallback error_callback_;
};

ZooKeeperProvider::ZooKeeperProvider(
    std::shared_ptr<ZooKeeperClient> client,
    std::string root_path
):state_(std::make_shared<State>(
      std::move(client),
      std::move(root_path)
  )){
    state_->Start();
}

ZooKeeperProvider::~ZooKeeperProvider()=default;

void ZooKeeperProvider::Register(
    std::string service_name,
    cluster::Endpoint endpoint
){
    state_->Register(
        std::move(service_name),
        std::move(endpoint)
    );
}

std::string ZooKeeperProvider::RegisteredNode(
    const std::string& service_name,
    const cluster::Endpoint& endpoint
)const{
    return state_->RegisteredNode(service_name,endpoint);
}

void ZooKeeperProvider::SetErrorCallback(ErrorCallback callback){
    state_->SetErrorCallback(std::move(callback));
}

std::string ZooKeeperProvider::ProviderParentPath(
    const std::string& root_path,
    const std::string& service_name
){
    ValidateServiceName(service_name);
    return NormalizeRootPath(root_path)+
           '/'+service_name+
           "/providers";
}

}
