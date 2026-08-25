#include "CalculatorService.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/rpc/RpcServer.h"

#include <cstdint>
#include <cstddef>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

using namespace asterrpc;
using namespace asterrpc::example::calculator;

namespace{

class CalculatorServiceImpl:public CalculatorService{
public:
    void Add(
        const AddRequest& request,
        AddResponse* response
    )override{
        int result=request.a()+request.b();
        response->set_result(result);
    }
};

struct Arguments{
    std::uint16_t port=9000;
    std::size_t io_threads=0;
    std::size_t business_threads=0;
    std::size_t business_queue_capacity=65536;
    net::IoLoopLoadBalance io_load_balance=
        net::IoLoopLoadBalance::RoundRobin;
};

std::size_t ParseSize(const std::string& value,const char* option){
    if(value.empty()||value.front()=='-'){
        throw std::invalid_argument(
            std::string(option)+" requires a non-negative integer"
        );
    }

    std::size_t parsed=0;
    unsigned long long result=std::stoull(value,&parsed);
    if(parsed!=value.size()||
       result>std::numeric_limits<std::size_t>::max()){
        throw std::invalid_argument(
            std::string(option)+" requires a valid integer"
        );
    }
    return static_cast<std::size_t>(result);
}

Arguments ParseArguments(int argc,char* argv[]){
    Arguments arguments;
    int index=1;

    if(index<argc&&std::string(argv[index]).rfind("--",0)!=0){
        std::size_t port=ParseSize(argv[index],"port");
        if(port==0||port>65535){
            throw std::invalid_argument(
                "port must be between 1 and 65535"
            );
        }
        arguments.port=static_cast<std::uint16_t>(port);
        ++index;
    }

    while(index<argc){
        std::string option=argv[index++];
        if(index>=argc){
            throw std::invalid_argument("missing value for "+option);
        }
        std::string value=argv[index++];

        if(option=="--io-threads"){
            arguments.io_threads=ParseSize(value,option.c_str());
        }else if(option=="--business-threads"){
            arguments.business_threads=ParseSize(
                value,
                option.c_str()
            );
        }else if(option=="--business-queue"){
            arguments.business_queue_capacity=ParseSize(
                value,
                option.c_str()
            );
        }else if(option=="--io-balance"){
            if(value=="round-robin"){
                arguments.io_load_balance=
                    net::IoLoopLoadBalance::RoundRobin;
            }else if(value=="least-connections"){
                arguments.io_load_balance=
                    net::IoLoopLoadBalance::LeastConnections;
            }else{
                throw std::invalid_argument(
                    "--io-balance must be round-robin or "
                    "least-connections"
                );
            }
        }else{
            throw std::invalid_argument("unknown option: "+option);
        }
    }

    if(arguments.business_threads!=0&&
       arguments.business_queue_capacity==0){
        throw std::invalid_argument(
            "--business-queue must be positive when "
            "business threads are enabled"
        );
    }

    return arguments;
}

}

int main(int argc,char* argv[]){
    try{
        Arguments arguments=ParseArguments(argc,argv);

        net::EventLoop loop;
        net::InetAddress address("0.0.0.0",arguments.port);
        rpc::RpcServerOptions server_options;
        server_options.tcp.io_threads=arguments.io_threads;
        server_options.tcp.io_load_balance=
            arguments.io_load_balance;
        server_options.business_threads=arguments.business_threads;
        server_options.business_queue_capacity=
            arguments.business_queue_capacity;
        rpc::RpcServer server(&loop,address,server_options);

        CalculatorServiceImpl service;
        CalculatorServiceAdapter adapter(&service);
        adapter.RegisterTo(&server);
        server.RegisterMethod(
            "BenchService",
            "Echo",
            [](const std::string& payload){
                return payload;
            }
        );
        server.Start();

        std::cout<<"calculator server listening on 0.0.0.0:"
                 <<arguments.port<<'\n'
                 <<"io threads: "<<arguments.io_threads<<'\n'
                 <<"business threads: "
                 <<arguments.business_threads<<'\n'
                 <<"io balance: "
                 <<(
                     arguments.io_load_balance==
                         net::IoLoopLoadBalance::LeastConnections
                     ?"least-connections"
                     :"round-robin"
                 )<<'\n'
                 <<"press Enter to stop the server\n";

        std::thread stop_thread([&loop](){
            std::string line;
            std::getline(std::cin,line);
            loop.Stop();
        });

        loop.Loop();
        stop_thread.join();

        return 0;
    }catch(const std::exception& error){
        std::cerr<<"server error: "<<error.what()<<'\n';
        return 1;
    }
}
