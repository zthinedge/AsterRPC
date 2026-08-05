#include "CalculatorService.h"
#include "asterrpc/log/AsyncLogger.h"
#include "asterrpc/log/LogMacros.h"
#include "asterrpc/metrics/RpcMetrics.h"
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
    explicit CalculatorServiceImpl(log::AsyncLogger* logger)
        :logger_(logger){}

    void Add(
        const AddRequest& request,
        AddResponse* response
    )override{
        int result=request.a()+request.b();
        response->set_result(result);

        ASTERRPC_LOG_INFO(
            *logger_,
            "CalculatorService.Add: "+
            std::to_string(request.a())+'+'+
            std::to_string(request.b())+'='+
            std::to_string(result)
        );
    }

private:
    log::AsyncLogger* logger_;
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

void PrintMetrics(const metrics::RpcMetricsSnapshot& snapshot){
    std::cout<<"\n[server metrics]\n"
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
        Arguments arguments=ParseArguments(argc,argv);

        log::LoggerOptions log_options;
        log_options.file_path="logs/calculator_server.log";
        log_options.roll_size_bytes=1024*1024;
        log::AsyncLogger logger(log_options);

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

        CalculatorServiceImpl service(&logger);
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

        ASTERRPC_LOG_INFO(
            logger,
            "calculator rpc server listening on port "+
            std::to_string(arguments.port)
        );

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

        PrintMetrics(server.GetMetrics());

        ASTERRPC_LOG_INFO(logger,"calculator rpc server stopped");
        logger.Stop();
        return 0;
    }catch(const std::exception& error){
        std::cerr<<"server error: "<<error.what()<<'\n';
        return 1;
    }
}
