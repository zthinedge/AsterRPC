#include "CalculatorStub.h"
#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/rpc/CallOptions.h"
#include "asterrpc/rpc/RpcClient.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace asterrpc;
using namespace asterrpc::example::calculator;

namespace{

struct ClientArguments{
    std::string host="127.0.0.1";
    std::uint16_t port=9000;
    int left=20;
    int right=22;
};

ClientArguments ParseArguments(int argc,char* argv[]){
    ClientArguments arguments;

    if(argc>1){
        arguments.host=argv[1];
    }
    if(argc>2){
        int port=std::stoi(argv[2]);
        if(port<1||port>65535){
            throw std::invalid_argument(
                "port must be between 1 and 65535"
            );
        }
        arguments.port=static_cast<std::uint16_t>(port);
    }
    if(argc>3){
        arguments.left=std::stoi(argv[3]);
    }
    if(argc>4){
        arguments.right=std::stoi(argv[4]);
    }

    return arguments;
}

}

int main(int argc,char* argv[]){
    try{
        ClientArguments arguments=ParseArguments(argc,argv);

        net::EventLoop loop;
        net::InetAddress server_address(arguments.host,arguments.port);
        rpc::RpcClient client(&loop,server_address);
        CalculatorStub stub(&client);
        std::thread call_thread;
        int exit_code=1;

        client.SetConnectionCallback([&](){
            call_thread=std::thread([&](){
                try{
                    AddRequest request;
                    request.set_a(arguments.left);
                    request.set_b(arguments.right);

                    rpc::CallOptions options;
                    options.timeout=std::chrono::seconds(2);
                    AddResponse response=stub.Add(request,options);

                    std::cout<<arguments.left<<" + "
                             <<arguments.right<<" = "
                             <<response.result()<<'\n';

                    exit_code=0;
                }catch(const std::exception& error){
                    std::cerr<<"rpc call failed: "<<error.what()<<'\n';
                }

                client.Disconnect();
            });
        });

        client.SetCloseCallback([&loop](){
            loop.Stop();
        });

        client.SetErrorCallback([&](int error){
            std::string message=
                "connect failed: "+std::string(std::strerror(error));
            std::cerr<<message<<'\n';
            loop.Stop();
        });

        client.Connect();
        loop.Loop();

        if(call_thread.joinable()){
            call_thread.join();
        }

        return exit_code;
    }catch(const std::exception& error){
        std::cerr<<"client error: "<<error.what()<<'\n';
        return 1;
    }
}
