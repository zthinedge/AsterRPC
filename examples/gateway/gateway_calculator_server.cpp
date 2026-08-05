#include "CalculatorService.h"
#include "asterrpc/cluster/Endpoint.h"
#include "asterrpc/log/AsyncLogger.h"
#include "asterrpc/log/LogMacros.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/registry/ZooKeeperClient.h"
#include "asterrpc/registry/ZooKeeperProvider.h"
#include "asterrpc/rpc/RpcServer.h"

#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <thread>

using namespace asterrpc;
using namespace asterrpc::example::calculator;

namespace{

struct Arguments{
    std::uint16_t port=9000;
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

class CalculatorServiceImpl:public CalculatorService{
public:
    explicit CalculatorServiceImpl(log::AsyncLogger* logger)
        :logger_(logger){}

    void Add(
        const AddRequest& request,
        AddResponse* response
    )override{
        response->set_result(request.a()+request.b());
        ASTERRPC_LOG_INFO(
            *logger_,
            "gateway backend CalculatorService.Add result="+
            std::to_string(response->result())
        );
    }

private:
    log::AsyncLogger* logger_;
};

}

int main(int argc,char* argv[]){
    try{
        Arguments arguments=ParseArguments(argc,argv);
        sigset_t stop_signals=BlockStopSignals();

        registry::ZooKeeperClientOptions options;
        options.servers=arguments.zookeeper_servers;
        auto zk_client=
            std::make_shared<registry::ZooKeeperClient>(options);
        zk_client->Start();
        if(!zk_client->WaitUntilConnected(std::chrono::seconds(10))){
            throw std::runtime_error("ZooKeeper connect timeout");
        }

        log::LoggerOptions logger_options;
        logger_options.file_path=
            "logs/gateway_calculator_server.log";
        log::AsyncLogger logger(logger_options);
        net::EventLoop loop;
        rpc::RpcServer server(
            &loop,
            net::InetAddress("0.0.0.0",arguments.port)
        );
        CalculatorServiceImpl service(&logger);
        CalculatorServiceAdapter adapter(&service);
        adapter.RegisterTo(&server);
        server.Start();

        registry::ZooKeeperProvider provider(zk_client);
        provider.Register(
            kCalculatorServiceName,
            cluster::Endpoint("127.0.0.1",arguments.port)
        );

        std::cout<<"gateway Calculator RPC backend ready on "
                 <<"0.0.0.0:"<<arguments.port<<'\n';

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
        std::cerr<<"gateway backend error: "<<error.what()<<'\n';
        return 1;
    }
}
