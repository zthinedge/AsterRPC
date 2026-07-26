#include "calculator.pb.h"
#include "minirpc/cluster/ChannelManager.h"
#include "minirpc/cluster/RoundRobin.h"
#include "minirpc/gateway/HttpGatewayServer.h"
#include "minirpc/gateway/RpcChannel.h"
#include "minirpc/log/AsyncLogger.h"
#include "minirpc/log/LogMacros.h"
#include "minirpc/net/EventLoop.h"
#include "minirpc/net/InetAddress.h"
#include "minirpc/registry/ZooKeeperClient.h"
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
#include <utility>

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

class DiscoveryRpcChannel:public gateway::RpcChannel{
public:
    DiscoveryRpcChannel(
        registry::ZooKeeperDiscovery* discovery,
        cluster::ChannelManager* channels,
        log::AsyncLogger* logger
    ):discovery_(discovery),
      channels_(channels),
      logger_(logger){
        if(discovery_==nullptr||
           channels_==nullptr||
           logger_==nullptr){
            throw std::invalid_argument(
                "gateway channel dependency is null"
            );
        }
    }

    void AsyncCall(
        std::string service_name,
        std::string method_name,
        std::string payload,
        rpc::CallOptions options,
        Completion completion
    )override{
        registry::DiscoveryResult discovered=
            discovery_->Resolve(service_name);
        auto endpoint=round_robin_.Select(discovered.providers);
        if(!endpoint){
            completion(ErrorResponse(
                protocol::StatusCode::ConnectionFailed,
                discovered.status==
                    registry::DiscoveryStatus::NoProvider
                ?"no provider for "+service_name
                :"service discovery is not ready"
            ));
            return;
        }

        MINIRPC_LOG_INFO(
            *logger_,
            "HTTP gateway calling "+service_name+'.'+method_name+
            " endpoint="+endpoint->ToString()
        );

        try{
            channels_->GetOrCreate(*endpoint)->AsyncCall(
                std::move(service_name),
                std::move(method_name),
                std::move(payload),
                completion,
                options
            );
        }catch(const std::exception& error){
            completion(ErrorResponse(
                protocol::StatusCode::ConnectionFailed,
                error.what()
            ));
        }
    }

private:
    registry::ZooKeeperDiscovery* discovery_;
    cluster::ChannelManager* channels_;
    log::AsyncLogger* logger_;
    cluster::RoundRobin round_robin_;
};

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

        zk_client->Start();
        if(!zk_client->WaitUntilConnected(std::chrono::seconds(10))){
            throw std::runtime_error("ZooKeeper connect timeout");
        }
        discovery.WatchService(std::string(
            calculator->full_name().data(),
            calculator->full_name().size()
        ));

        net::EventLoop loop;
        cluster::ChannelManager channels(&loop);
        log::LoggerOptions logger_options;
        logger_options.file_path="logs/http_gateway.log";
        log::AsyncLogger logger(logger_options);
        DiscoveryRpcChannel rpc_channel(
            &discovery,
            &channels,
            &logger
        );

        gateway::HttpGatewayServer server(
            &loop,
            net::InetAddress("0.0.0.0",arguments.port),
            &rpc_channel
        );
        server.RegisterService(calculator);
        server.Start();

        std::cout<<"HTTP gateway ready on 0.0.0.0:"
                 <<arguments.port<<'\n'
                 <<"POST /rpc/CalculatorService/Add\n";

        std::thread stop_thread([&loop,stop_signals]()mutable{
            int signal=0;
            sigwait(&stop_signals,&signal);
            loop.Stop();
        });

        loop.Loop();
        zk_client->Close();
        stop_thread.join();
        logger.Stop();
        return 0;
    }catch(const std::exception& error){
        std::cerr<<"HTTP gateway error: "<<error.what()<<'\n';
        return 1;
    }
}
