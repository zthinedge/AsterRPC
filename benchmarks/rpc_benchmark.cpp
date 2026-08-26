#include "asterrpc/net/EventLoop.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/protocol/RpcMessage.h"
#include "asterrpc/rpc/CallOptions.h"
#include "asterrpc/rpc/RpcClient.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace asterrpc;

namespace{

using Clock=std::chrono::steady_clock;

constexpr const char* kBenchmarkService="BenchService";
constexpr const char* kBenchmarkMethod="Echo";

struct Arguments{
    std::string host="127.0.0.1";
    std::uint16_t port=9000;
    std::size_t connections=1;
    std::size_t concurrency=1;
    std::size_t requests=10000;
    std::size_t payload_bytes=1024;
    std::size_t deadline_ms=1000;
    bool json=false;
    bool help=false;
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

    for(int index=1;index<argc;){
        std::string option=argv[index++];

        if(option=="--help"||option=="-h"){
            arguments.help=true;
            continue;
        }
        if(option=="--json"){
            arguments.json=true;
            continue;
        }
        if(index>=argc){
            throw std::invalid_argument("missing value for "+option);
        }

        std::string value=argv[index++];
        if(option=="--host"){
            arguments.host=std::move(value);
        }else if(option=="--port"){
            std::size_t port=ParseSize(value,option.c_str());
            if(port==0||port>65535){
                throw std::invalid_argument(
                    "--port must be between 1 and 65535"
                );
            }
            arguments.port=static_cast<std::uint16_t>(port);
        }else if(option=="--connections"){
            arguments.connections=ParseSize(value,option.c_str());
        }else if(option=="--concurrency"){
            arguments.concurrency=ParseSize(value,option.c_str());
        }else if(option=="--requests"){
            arguments.requests=ParseSize(value,option.c_str());
        }else if(option=="--payload-bytes"){
            arguments.payload_bytes=ParseSize(value,option.c_str());
        }else if(option=="--deadline-ms"){
            arguments.deadline_ms=ParseSize(value,option.c_str());
        }else{
            throw std::invalid_argument("unknown option: "+option);
        }
    }

    if(arguments.help){
        return arguments;
    }
    if(arguments.host.empty()){
        throw std::invalid_argument("--host must not be empty");
    }
    if(arguments.connections==0){
        throw std::invalid_argument("--connections must be positive");
    }
    if(arguments.concurrency==0){
        throw std::invalid_argument("--concurrency must be positive");
    }
    if(arguments.requests==0){
        throw std::invalid_argument("--requests must be positive");
    }
    if(arguments.connections>arguments.concurrency){
        throw std::invalid_argument(
            "--connections must not exceed --concurrency"
        );
    }
    if(arguments.concurrency>arguments.requests){
        throw std::invalid_argument(
            "--concurrency must not exceed --requests"
        );
    }

    return arguments;
}

void PrintUsage(const char* program){
    std::cout
        <<"usage: "<<program<<" [options]\n"
        <<"  --host HOST           server host (default: 127.0.0.1)\n"
        <<"  --port PORT           server port (default: 9000)\n"
        <<"  --connections N       TCP connections (default: 1)\n"
        <<"  --concurrency N       total in-flight RPCs (default: 1)\n"
        <<"  --requests N          total RPCs (default: 10000)\n"
        <<"  --payload-bytes N     Echo payload size (default: 1024)\n"
        <<"  --deadline-ms N       per-call timeout, 0 disables it "
          "(default: 1000)\n"
        <<"  --json                print one JSON result object\n";
}

std::string EscapeJson(const std::string& value){
    std::ostringstream output;
    for(unsigned char character:value){
        switch(character){
        case '"': output<<"\\\""; break;
        case '\\': output<<"\\\\"; break;
        case '\n': output<<"\\n"; break;
        case '\r': output<<"\\r"; break;
        case '\t': output<<"\\t"; break;
        default:
            if(character<0x20){
                output<<"\\u"
                      <<std::hex<<std::setw(4)<<std::setfill('0')
                      <<static_cast<int>(character)
                      <<std::dec<<std::setfill(' ');
            }else{
                output<<static_cast<char>(character);
            }
        }
    }
    return output.str();
}

double Percentile(std::vector<double> values,double percentile){
    if(values.empty()){
        return 0.0;
    }

    std::sort(values.begin(),values.end());
    std::size_t rank=static_cast<std::size_t>(
        std::ceil(percentile*static_cast<double>(values.size()))
    );
    rank=std::max<std::size_t>(1,rank);
    return values[rank-1];
}

class RpcBenchmark{
public:
    RpcBenchmark(net::EventLoop* loop,Arguments arguments)
        :loop_(loop),
         arguments_(std::move(arguments)),
         payload_(arguments_.payload_bytes,'x'),
         connected_(arguments_.connections,false){
        clients_.reserve(arguments_.connections);
        successful_latencies_ms_.reserve(arguments_.requests);

        net::InetAddress address(arguments_.host,arguments_.port);
        for(std::size_t index=0;index<arguments_.connections;++index){
            auto client=std::make_unique<rpc::RpcClient>(loop_,address);
            client->SetConnectionCallback([this,index](){
                HandleConnected(index);
            });
            client->SetCloseCallback([this,index](){
                HandleClosed(index);
            });
            client->SetErrorCallback([this,index](int error){
                HandleConnectionError(index,error);
            });
            clients_.push_back(std::move(client));
        }
    }

    void Start(){
        connect_timer_=loop_->RunAfter(
            std::chrono::seconds(5),
            [this](){
                if(state_==State::Connecting){
                    Fail("timed out while connecting to RPC server");
                }
            }
        );

        for(auto& client:clients_){
            client->Connect();
        }
    }

    int ExitCode()const noexcept{
        return exit_code_;
    }

private:
    enum class State{
        Connecting,
        Running,
        Finished,
        Failed
    };

    void HandleConnected(std::size_t index){
        if(state_!=State::Connecting||connected_[index]){
            return;
        }

        connected_[index]=true;
        ++connected_count_;
        if(connected_count_!=clients_.size()){
            return;
        }

        loop_->CancelTimer(connect_timer_);
        state_=State::Running;
        started_at_=Clock::now();

        const std::size_t initial=std::min(
            arguments_.concurrency,
            arguments_.requests
        );
        for(std::size_t index=0;index<initial;++index){
            if(!SubmitOne()){
                return;
            }
        }
    }

    void HandleClosed(std::size_t index){
        if(connected_[index]){
            connected_[index]=false;
            --connected_count_;
        }

        if(state_==State::Running&&!HasConnectedClient()){
            Fail("all RPC connections were closed");
        }
    }

    void HandleConnectionError(std::size_t index,int error){
        std::ostringstream message;
        message<<"connection "<<index<<" failed: "
               <<std::strerror(error);
        Fail(message.str());
    }

    bool HasConnectedClient()const{
        for(const auto& client:clients_){
            if(client->IsConnected()){
                return true;
            }
        }
        return false;
    }

    rpc::RpcClient* NextConnectedClient(){
        for(std::size_t attempt=0;attempt<clients_.size();++attempt){
            const std::size_t index=next_client_%clients_.size();
            ++next_client_;
            if(clients_[index]->IsConnected()){
                return clients_[index].get();
            }
        }
        return nullptr;
    }

    bool SubmitOne(){
        if(state_!=State::Running||sent_>=arguments_.requests){
            return false;
        }

        rpc::RpcClient* client=NextConnectedClient();
        if(client==nullptr){
            Fail("no connected RPC client is available");
            return false;
        }

        rpc::CallOptions options;
        if(arguments_.deadline_ms!=0){
            options.timeout=std::chrono::milliseconds(
                arguments_.deadline_ms
            );
        }

        const auto request_started_at=Clock::now();
        ++sent_;
        ++inflight_;

        try{
            client->AsyncCall(
                kBenchmarkService,
                kBenchmarkMethod,
                payload_,
                [this,request_started_at](protocol::RpcMessage response){
                    HandleResponse(
                        request_started_at,
                        std::move(response)
                    );
                },
                options
            );
        }catch(const std::exception& error){
            --inflight_;
            Fail(std::string("failed to submit RPC: ")+error.what());
            return false;
        }

        return true;
    }

    void HandleResponse(
        Clock::time_point request_started_at,
        protocol::RpcMessage response
    ){
        if(state_!=State::Running){
            return;
        }

        --inflight_;
        ++completed_;

        const double latency_ms=
            std::chrono::duration<double,std::milli>(
                Clock::now()-request_started_at
            ).count();

        if(response.meta.status_code==protocol::StatusCode::Ok){
            if(response.payload==payload_){
                ++success_;
                successful_latencies_ms_.push_back(latency_ms);
            }else{
                ++payload_mismatches_;
            }
        }else if(
            response.meta.status_code==protocol::StatusCode::Timeout
        ){
            ++timeouts_;
        }else if(
            response.meta.status_code==
                protocol::StatusCode::ConnectionFailed
        ){
            ++connection_failures_;
        }else{
            ++rpc_errors_;
        }

        if(completed_==arguments_.requests){
            Finish();
            return;
        }

        if(sent_<arguments_.requests){
            SubmitOne();
        }
    }

    void Finish(){
        if(state_!=State::Running){
            return;
        }

        state_=State::Finished;
        const auto finished_at=Clock::now();
        const double duration_ms=
            std::chrono::duration<double,std::milli>(
                finished_at-started_at_
            ).count();
        const double qps=duration_ms==0.0
            ?0.0
            :static_cast<double>(completed_)*1000.0/duration_ms;
        const double p50=Percentile(successful_latencies_ms_,0.50);
        const double p99=Percentile(successful_latencies_ms_,0.99);
        const std::size_t failed=completed_-success_;

        if(arguments_.json){
            std::cout<<std::fixed<<std::setprecision(3)
                     <<'{'
                     <<"\"connections\":"<<arguments_.connections<<','
                     <<"\"concurrency\":"<<arguments_.concurrency<<','
                     <<"\"requests\":"<<completed_<<','
                     <<"\"payload_bytes\":"
                     <<arguments_.payload_bytes<<','
                     <<"\"deadline_ms\":"<<arguments_.deadline_ms<<','
                     <<"\"success\":"<<success_<<','
                     <<"\"failed\":"<<failed<<','
                     <<"\"timeouts\":"<<timeouts_<<','
                     <<"\"connection_failures\":"
                     <<connection_failures_<<','
                     <<"\"rpc_errors\":"<<rpc_errors_<<','
                     <<"\"payload_mismatches\":"
                     <<payload_mismatches_<<','
                     <<"\"duration_ms\":"<<duration_ms<<','
                     <<"\"qps\":"<<qps<<','
                     <<"\"p50_ms\":"<<p50<<','
                     <<"\"p99_ms\":"<<p99
                     <<"}\n";
        }else{
            std::cout<<std::fixed<<std::setprecision(3)
                     <<"connections: "<<arguments_.connections<<'\n'
                     <<"concurrency: "<<arguments_.concurrency<<'\n'
                     <<"requests: "<<completed_<<'\n'
                     <<"payload bytes: "<<arguments_.payload_bytes<<'\n'
                     <<"success: "<<success_<<'\n'
                     <<"failed: "<<failed<<'\n'
                     <<"timeouts: "<<timeouts_<<'\n'
                     <<"connection failures: "
                     <<connection_failures_<<'\n'
                     <<"rpc errors: "<<rpc_errors_<<'\n'
                     <<"payload mismatches: "
                     <<payload_mismatches_<<'\n'
                     <<"duration: "<<duration_ms<<" ms\n"
                     <<"qps: "<<qps<<'\n'
                     <<"p50: "<<p50<<" ms\n"
                     <<"p99: "<<p99<<" ms\n";
        }

        exit_code_=0;
        loop_->Stop();
    }

    void Fail(const std::string& error){
        if(state_==State::Finished||state_==State::Failed){
            return;
        }

        state_=State::Failed;
        exit_code_=1;
        if(arguments_.json){
            std::cout<<"{\"error\":\""
                     <<EscapeJson(error)
                     <<"\"}\n";
        }else{
            std::cerr<<"benchmark failed: "<<error<<'\n';
        }
        loop_->Stop();
    }

    net::EventLoop* loop_;
    Arguments arguments_;
    std::string payload_;
    std::vector<std::unique_ptr<rpc::RpcClient>> clients_;
    std::vector<bool> connected_;
    std::vector<double> successful_latencies_ms_;

    State state_=State::Connecting;
    net::EventLoop::TimerId connect_timer_=0;
    Clock::time_point started_at_{};
    std::size_t connected_count_=0;
    std::size_t next_client_=0;
    std::size_t sent_=0;
    std::size_t inflight_=0;
    std::size_t completed_=0;
    std::size_t success_=0;
    std::size_t timeouts_=0;
    std::size_t connection_failures_=0;
    std::size_t rpc_errors_=0;
    std::size_t payload_mismatches_=0;
    int exit_code_=1;
};

}

int main(int argc,char* argv[]){
    try{
        Arguments arguments=ParseArguments(argc,argv);
        if(arguments.help){
            PrintUsage(argv[0]);
            return 0;
        }

        net::EventLoop loop;
        RpcBenchmark benchmark(&loop,std::move(arguments));
        benchmark.Start();
        loop.Loop();
        return benchmark.ExitCode();
    }catch(const std::exception& error){
        std::cerr<<"benchmark error: "<<error.what()<<'\n';
        return 1;
    }
}
