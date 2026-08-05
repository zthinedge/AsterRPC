#include "CalculatorStub.h"
#include "asterrpc/log/AsyncLogger.h"
#include "asterrpc/log/LogMacros.h"
#include "asterrpc/metrics/RpcMetrics.h"
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

void PrintMetrics(const metrics::RpcMetricsSnapshot& snapshot){
    std::cout<<"\n[client metrics]\n"
             <<"total requests: "<<snapshot.total_requests<<'\n'
             <<"successful requests: "
             <<snapshot.successful_requests<<'\n'
             <<"failed requests: "<<snapshot.failed_requests<<'\n'
             <<"timeout requests: "<<snapshot.timeout_requests<<'\n'
             <<"retries: "<<snapshot.retries<<'\n'
             <<"inflight requests: "<<snapshot.inflight_requests<<'\n'
             <<"active connections: "<<snapshot.active_connections<<'\n'
             <<"average latency(us): "
             <<snapshot.AverageLatencyMicros()<<'\n'
             <<"max latency(us): "<<snapshot.max_latency_us<<'\n'
             <<"P50/P95/P99(us): "
             <<snapshot.p50_latency_us<<'/'
             <<snapshot.p95_latency_us<<'/'
             <<snapshot.p99_latency_us<<'\n';
}

}

int main(int argc,char* argv[]){
    try{
        ClientArguments arguments=ParseArguments(argc,argv);

        log::LoggerOptions log_options;
        log_options.file_path="logs/calculator_client.log";
        log::AsyncLogger logger(log_options);

        net::EventLoop loop;
        net::InetAddress server_address(arguments.host,arguments.port);
        rpc::RpcClient client(&loop,server_address);
        CalculatorStub stub(&client);
        std::thread call_thread;
        int exit_code=1;

        client.SetConnectionCallback([&](){
            ASTERRPC_LOG_INFO(logger,"connected to calculator server");

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

                    ASTERRPC_LOG_INFO(
                        logger,
                        "rpc result="+std::to_string(response.result())
                    );
                    exit_code=0;
                }catch(const std::exception& error){
                    std::cerr<<"rpc call failed: "<<error.what()<<'\n';
                    ASTERRPC_LOG_ERROR(logger,error.what());
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
            ASTERRPC_LOG_ERROR(logger,message);
            loop.Stop();
        });

        client.Connect();
        loop.Loop();

        if(call_thread.joinable()){
            call_thread.join();
        }

        PrintMetrics(client.GetMetrics());

        logger.Stop();
        return exit_code;
    }catch(const std::exception& error){
        std::cerr<<"client error: "<<error.what()<<'\n';
        return 1;
    }
}
