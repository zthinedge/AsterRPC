#include "GatewayRuntime.h"
#include "calculator.pb.h"
#include "minirpc/cluster/ChannelManager.h"
#include "minirpc/gateway/HttpGatewayServer.h"
#include "minirpc/log/AsyncLogger.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/net/InetAddress.h"
#include "minirpc/registry/ZooKeeperClient.h"
#include "minirpc/registry/ZooKeeperConfigCenter.h"
#include "minirpc/registry/ZooKeeperDiscovery.h"

#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <thread>

using namespace minirpc;
using namespace minirpc::example::calculator;

namespace{

const google::protobuf::ServiceDescriptor* CalculatorDescriptor(){
    return AddRequest::descriptor()->file()->FindServiceByName(
        "CalculatorService"
    );
}

struct Arguments{
    std::uint16_t port=8080;
    std::string zookeeper_servers="127.0.0.1:2181";
};

Arguments ParseArguments(int argc,char* argv[]){
    Arguments arguments;
    if(argc>1){
        int port=std::stoi(argv[1]);
        if(port<1||port>65535){
            throw std::invalid_argument(
                "port must be between 1 and 65535"
            );
        }
        arguments.port=static_cast<std::uint16_t>(port);
    }
    if(argc>2){
        arguments.zookeeper_servers=argv[2];
    }
    return arguments;
}

sigset_t BlockStopSignals(){
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals,SIGINT);
    sigaddset(&signals,SIGTERM);
    if(pthread_sigmask(SIG_BLOCK,&signals,nullptr)!=0){
        throw std::runtime_error("failed to block stop signals");
    }
    return signals;
}

}

int main(int argc,char* argv[]){
    try{
        Arguments arguments=ParseArguments(argc,argv);
        sigset_t stop_signals=BlockStopSignals();
        const google::protobuf::ServiceDescriptor* calculator=
            CalculatorDescriptor();
        if(calculator==nullptr){
            throw std::runtime_error(
                "CalculatorService descriptor is unavailable"
            );
        }

        registry::ZooKeeperClientOptions options;
        options.servers=arguments.zookeeper_servers;
        auto zk_client=
            std::make_shared<registry::ZooKeeperClient>(options);
        registry::ZooKeeperDiscovery discovery(zk_client);
        registry::ZooKeeperConfigCenter config_center(zk_client);
        std::string service_name(
            calculator->full_name().data(),
            calculator->full_name().size()
        );

        zk_client->Start();
        if(!zk_client->WaitUntilConnected(std::chrono::seconds(10))){
            throw std::runtime_error("ZooKeeper connect timeout");
        }
        discovery.WatchService(service_name);
        config_center.WatchService(service_name);

        net::EventLoop loop;
        cluster::ChannelManager channels(&loop);
        log::LoggerOptions logger_options;
        logger_options.file_path="logs/http_gateway.log";
        log::AsyncLogger logger(logger_options);
        example::gateway::GatewayRuntime runtime(
            zk_client,
            &discovery,
            &config_center,
            &channels,
            &logger,
            calculator
        );
        runtime.Start(&loop);

        gateway::HttpGatewayServer server(
            &loop,
            net::InetAddress("0.0.0.0",arguments.port),
            &runtime
        );
        server.RegisterService(calculator);
        server.SetAdminDataSource(&runtime);
        server.Start();

        std::cout<<"HTTP gateway ready on 0.0.0.0:"
                 <<arguments.port<<'\n'
                 <<"POST /rpc/CalculatorService/Add\n"
                 <<"GET  /admin/api/health\n";

        std::thread stop_thread([&loop,stop_signals]()mutable{
            int signal=0;
            sigwait(&stop_signals,&signal);
            loop.Stop();
        });

        loop.Loop();
        stop_thread.join();
        runtime.Stop();
        zk_client->Close();
        logger.Stop();
        return 0;
    }catch(const std::exception& error){
        std::cerr<<"HTTP gateway error: "<<error.what()<<'\n';
        return 1;
    }
}
