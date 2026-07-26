#include "minirpc/cluster/Endpoint.h"
#include "minirpc/registry/ZooKeeperClient.h"
#include "minirpc/registry/ZooKeeperConfigCenter.h"
#include "minirpc/registry/ZooKeeperDiscovery.h"
#include "minirpc/registry/ZooKeeperProvider.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

using namespace minirpc;

namespace{

void TestProviderPaths(){
    assert(
        registry::ZooKeeperProvider::ProviderParentPath(
            "/mini-rpc/services",
            "UserService"
        )==
        "/mini-rpc/services/UserService/providers"
    );
    assert(
        registry::ZooKeeperProvider::ProviderParentPath(
            "/mini-rpc/services/",
            "OrderService"
        )==
        "/mini-rpc/services/OrderService/providers"
    );

    bool rejected=false;
    try{
        registry::ZooKeeperProvider::ProviderParentPath(
            "/mini-rpc/services",
            "bad/service"
        );
    }catch(const std::invalid_argument&){
        rejected=true;
    }
    assert(rejected);
}

void TestConfigPaths(){
    assert(registry::ZooKeeperConfigCenter::GlobalPath(
        "/mini-rpc/config"
    )=="/mini-rpc/config/global");
    assert(registry::ZooKeeperConfigCenter::ServicePath(
        "/mini-rpc/config/",
        "UserService"
    )=="/mini-rpc/config/UserService");

    bool rejected=false;
    try{
        registry::ZooKeeperConfigCenter::ServicePath(
            "/mini-rpc/config",
            "bad/service"
        );
    }catch(const std::invalid_argument&){
        rejected=true;
    }
    assert(rejected);
}

void TestOptionValidation(){
    registry::ZooKeeperClientOptions options;
    options.servers.clear();

    bool rejected=false;
    try{
        registry::ZooKeeperClient client(options);
    }catch(const std::invalid_argument&){
        rejected=true;
    }
    assert(rejected);
}

void TestDisconnectedClientLifecycle(){
    registry::ZooKeeperClientOptions options;
    options.servers="127.0.0.1:1";
    options.session_timeout=std::chrono::seconds(1);
    options.reconnect_delay=std::chrono::milliseconds(10);

    registry::ZooKeeperClient client(options);
    std::atomic_size_t state_changes{0};
    client.AddStateListener(
        [&state_changes](const registry::ZooKeeperConnectionEvent&){
            state_changes.fetch_add(1);
        }
    );

    client.Start();
    assert(
        !client.WaitUntilConnected(std::chrono::milliseconds(20))
    );
    client.Close();

    assert(
        client.CurrentEvent().state==
        registry::ZooKeeperConnectionState::Stopped
    );
    assert(state_changes.load()>0);

    client.Start();
    client.Close();
}

void TestDiscoveryNotReady(){
    auto client=std::make_shared<registry::ZooKeeperClient>();
    registry::ZooKeeperDiscovery discovery(client);

    discovery.WatchService("UserService");
    registry::DiscoveryResult result=
        discovery.Resolve("UserService");

    assert(result.status==registry::DiscoveryStatus::NotReady);
    assert(result.providers!=nullptr);
    assert(result.providers->empty());
}

bool WaitForProviderCount(
    const registry::ZooKeeperDiscovery& discovery,
    const std::string& service_name,
    std::size_t expected
){
    for(int attempt=0;attempt<100;++attempt){
        registry::DiscoveryResult result=
            discovery.Resolve(service_name);
        if(result.status==registry::DiscoveryStatus::Ok&&
           result.providers->size()==expected){
            return true;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10)
        );
    }
    return false;
}

template<class Predicate>
bool WaitUntil(
    Predicate predicate,
    std::chrono::milliseconds timeout
){
    auto deadline=std::chrono::steady_clock::now()+timeout;
    while(std::chrono::steady_clock::now()<deadline){
        if(predicate()){
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

void TestRealZooKeeper(const std::string& servers){
    registry::ZooKeeperClientOptions options;
    options.servers=servers;
    options.session_timeout=std::chrono::seconds(10);

    auto client=std::make_shared<registry::ZooKeeperClient>(options);
    client->Start();

    bool connected=client->WaitUntilConnected(
        std::chrono::seconds(10)
    );
    assert(connected);

    const std::string watch_probe_path=
        "/mini-rpc/config/watch-probe";
    client->EnsurePersistentPath(watch_probe_path);
    client->SetData(watch_probe_path,"before");
    std::atomic_bool data_watch_fired{false};
    assert(
        client->GetDataAndWatch(
            watch_probe_path,
            [&data_watch_fired](){
                data_watch_fired.store(
                    true,
                    std::memory_order_release
                );
            }
        )=="before"
    );
    client->SetData(watch_probe_path,"after");
    assert(WaitUntil(
        [&data_watch_fired](){
            return data_watch_fired.load(
                std::memory_order_acquire
            );
        },
        std::chrono::seconds(2)
    ));

    registry::ZooKeeperProvider provider(client);
    cluster::Endpoint user_endpoint("127.0.0.1",19001);
    cluster::Endpoint order_endpoint("127.0.0.1",19002);

    provider.Register("UserService",user_endpoint);
    provider.Register("OrderService",order_endpoint);

    registry::ZooKeeperDiscovery discovery(client);
    discovery.WatchService("UserService");
    discovery.WatchService("OrderService");
    discovery.WatchService("EmptyService");

    assert(WaitForProviderCount(discovery,"UserService",1));
    assert(WaitForProviderCount(discovery,"OrderService",1));

    registry::DiscoveryResult empty=
        discovery.Resolve("EmptyService");
    assert(empty.status==registry::DiscoveryStatus::NoProvider);
    assert(empty.providers->empty());

    std::string user_node=provider.RegisteredNode(
        "UserService",
        user_endpoint
    );
    std::string order_node=provider.RegisteredNode(
        "OrderService",
        order_endpoint
    );

    assert(
        user_node.find(
            "/mini-rpc/services/UserService/providers/instance-"
        )==0
    );
    assert(
        order_node.find(
            "/mini-rpc/services/OrderService/providers/instance-"
        )==0
    );

    provider.Register(
        "UserService",
        cluster::Endpoint("127.0.0.1",19003)
    );
    assert(WaitForProviderCount(discovery,"UserService",2));

    provider.Register(
        "UserService",
        cluster::Endpoint("127.0.0.1",19004)
    );
    assert(WaitForProviderCount(discovery,"UserService",3));

    client->EnsurePersistentPath("/mini-rpc/config/global");
    client->SetData(
        "/mini-rpc/config/global",
        R"({"default_timeout_ms":1001,"retry_count":2})"
    );
    client->EnsurePersistentPath(
        "/mini-rpc/config/UserService"
    );
    client->SetData(
        "/mini-rpc/config/UserService",
        "{}"
    );

    registry::ZooKeeperConfigCenter config_center(client);
    config_center.WatchService("UserService");
    assert(WaitUntil(
        [&config_center](){
            auto config=config_center.Get("UserService");
            return config->default_timeout==
                       std::chrono::milliseconds(1001)&&
                   config->retry_count==2;
        },
        std::chrono::seconds(2)
    ));

    client->SetData(
        "/mini-rpc/config/UserService",
        R"({"default_timeout_ms":201,"ewma_alpha":0.51})"
    );
    assert(WaitUntil(
        [&config_center](){
            auto config=config_center.Get("UserService");
            return config->default_timeout==
                       std::chrono::milliseconds(201)&&
                   config->retry_count==2&&
                   config->ewma_alpha==0.51;
        },
        std::chrono::seconds(2)
    ));

    client->Close();
}

}

int main(){
    TestProviderPaths();
    TestConfigPaths();
    TestOptionValidation();
    TestDisconnectedClientLifecycle();
    TestDiscoveryNotReady();

    const char* servers=std::getenv(
        "MINIRPC_ZOOKEEPER_TEST_SERVERS"
    );
    if(servers==nullptr||*servers=='\0'){
        std::cout
            <<"ZooKeeper integration test skipped; set "
            <<"MINIRPC_ZOOKEEPER_TEST_SERVERS to enable it\n";
        return 0;
    }

    TestRealZooKeeper(servers);
    std::cout<<"ZooKeeper registry tests passed\n";
    return 0;
}
