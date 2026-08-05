#include "asterrpc/health/HealthChecker.h"

#include "asterrpc/net/EventLoop.h"

#include <atomic>
#include <cmath>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace asterrpc::health{

namespace{

void ValidateActiveOptions(const ActiveHealthCheckOptions& options){
    if(options.interval.count()<=0){
        throw std::invalid_argument(
            "health check interval must be positive"
        );
    }
    if(options.timeout.count()<=0){
        throw std::invalid_argument(
            "health check timeout must be positive"
        );
    }
}

}

class HealthChecker::Impl:
    public std::enable_shared_from_this<HealthChecker::Impl>{
public:
    using TimerId=net::EventLoop::TimerId;

    explicit Impl(EndpointStateOptions options)
        :options_(options),
         state_snapshot_(std::make_shared<const StateList>()){}

    StateSnapshot Update(const EndpointSnapshot& endpoints){
        std::lock_guard<std::mutex> lock(mutex_);
        if(endpoints==endpoint_snapshot_){
            return state_snapshot_;
        }

        endpoint_snapshot_=endpoints;
        StateList result;

        if(endpoints==nullptr||endpoints->empty()){
            states_.clear();
            state_snapshot_=std::make_shared<const StateList>();
            return state_snapshot_;
        }

        std::unordered_set<
            cluster::Endpoint,
            cluster::EndpointHash
        > current;
        current.reserve(endpoints->size());
        result.reserve(endpoints->size());

        for(const auto& endpoint:*endpoints){
            if(!current.emplace(endpoint).second){
                continue;
            }

            auto state=states_.find(endpoint);
            if(state==states_.end()){
                EndpointStateOptions options=options_;
                auto weight=weights_.find(endpoint);
                if(weight!=weights_.end()){
                    options.weight=weight->second;
                }

                state=states_.emplace(
                    endpoint,
                    std::make_shared<EndpointState>(
                        endpoint,
                        options
                    )
                ).first;
            }
            result.push_back(state->second);
        }

        for(auto state=states_.begin();state!=states_.end();){
            if(current.find(state->first)==current.end()){
                state=states_.erase(state);
            }else{
                ++state;
            }
        }

        state_snapshot_=std::make_shared<const StateList>(
            std::move(result)
        );
        return state_snapshot_;
    }

    std::shared_ptr<EndpointState> Find(
        const cluster::Endpoint& endpoint
    )const{
        std::lock_guard<std::mutex> lock(mutex_);
        auto state=states_.find(endpoint);
        return state==states_.end()?nullptr:state->second;
    }

    void SetWeight(
        const cluster::Endpoint& endpoint,
        double weight
    ){
        if(!std::isfinite(weight)||weight<=0.0){
            throw std::invalid_argument(
                "endpoint weight must be positive"
            );
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto state=states_.find(endpoint);
        if(state!=states_.end()){
            state->second->SetWeight(weight);
        }
        weights_[endpoint]=weight;
    }

    void UpdateEndpointOptions(EndpointStateOptions options){
        ValidateEndpointStateOptions(options);

        std::lock_guard<std::mutex> lock(mutex_);
        options_=options;
        for(auto& item:states_){
            EndpointStateOptions endpoint_options=options_;
            auto weight=weights_.find(item.first);
            if(weight!=weights_.end()){
                endpoint_options.weight=weight->second;
            }
            item.second->UpdateOptions(endpoint_options);
        }
    }

    void UpdateActiveOptions(ActiveHealthCheckOptions options){
        ValidateActiveOptions(options);

        net::EventLoop* loop=nullptr;
        TimerId timer_id=0;
        std::uint64_t generation=0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            active_options_=options;
            if(!running_){
                return;
            }

            loop=loop_;
            timer_id=timer_id_;
            timer_id_=0;
            generation=++generation_;
        }

        if(timer_id!=0){
            loop->CancelTimer(timer_id);
        }

        std::weak_ptr<Impl> weak=shared_from_this();
        loop->RunInLoop([weak,generation](){
            if(auto self=weak.lock()){
                self->RunRound(generation);
            }
        });
    }

    std::size_t Size()const{
        std::lock_guard<std::mutex> lock(mutex_);
        return states_.size();
    }

    void Start(
        net::EventLoop* loop,
        ProbeFunction probe,
        ActiveHealthCheckOptions options
    ){
        if(loop==nullptr){
            throw std::invalid_argument(
                "health checker EventLoop is null"
            );
        }
        if(!probe){
            throw std::invalid_argument(
                "health checker probe is empty"
            );
        }
        ValidateActiveOptions(options);

        std::uint64_t generation=0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(running_){
                throw std::logic_error(
                    "health checker is already running"
                );
            }

            loop_=loop;
            probe_=std::move(probe);
            active_options_=options;
            running_=true;
            generation=++generation_;
        }

        std::weak_ptr<Impl> weak=shared_from_this();
        loop->RunInLoop([weak,generation](){
            if(auto self=weak.lock()){
                self->RunRound(generation);
            }
        });
    }

    void Stop()noexcept{
        net::EventLoop* loop=nullptr;
        TimerId timer_id=0;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!running_){
                return;
            }

            running_=false;
            ++generation_;
            loop=loop_;
            timer_id=timer_id_;
            timer_id_=0;
            probe_={};
        }

        if(loop!=nullptr&&timer_id!=0){
            loop->CancelTimer(timer_id);
        }
    }

    bool IsRunning()const noexcept{
        std::lock_guard<std::mutex> lock(mutex_);
        return running_;
    }

private:
    struct ProbeAttempt{
        std::atomic_bool completed{false};
        std::shared_ptr<EndpointState> state;
        net::EventLoop* loop=nullptr;
        TimerId timeout_timer=0;

        void Finish(bool success)noexcept{
            if(completed.exchange(true)){
                return;
            }

            if(loop!=nullptr&&timeout_timer!=0){
                loop->CancelTimer(timeout_timer);
            }
            state->CompleteHealthCheck(success);
        }
    };

    void RunRound(std::uint64_t generation){
        StateSnapshot states;
        ProbeFunction probe;
        ActiveHealthCheckOptions options;
        net::EventLoop* loop=nullptr;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!running_||generation!=generation_){
                return;
            }

            timer_id_=0;
            states=state_snapshot_;
            probe=probe_;
            options=active_options_;
            loop=loop_;
        }

        for(const auto& state:*states){
            if(!state->TryBeginHealthCheck()){
                continue;
            }

            auto attempt=std::make_shared<ProbeAttempt>();
            attempt->state=state;
            attempt->loop=loop;
            attempt->timeout_timer=loop->RunAfter(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    options.timeout
                ),
                [attempt](){
                    attempt->Finish(false);
                }
            );

            try{
                probe(
                    state->GetEndpoint(),
                    options.timeout,
                    [attempt](bool success){
                        attempt->Finish(success);
                    }
                );
            }catch(...){
                attempt->Finish(false);
            }
        }

        ScheduleNext(generation);
    }

    void ScheduleNext(std::uint64_t generation){
        std::lock_guard<std::mutex> lock(mutex_);
        if(!running_||generation!=generation_){
            return;
        }

        std::weak_ptr<Impl> weak=shared_from_this();
        timer_id_=loop_->RunAfter(
            std::chrono::duration_cast<std::chrono::microseconds>(
                active_options_.interval
            ),
            [weak,generation](){
                if(auto self=weak.lock()){
                    self->RunRound(generation);
                }
            }
        );
    }

    EndpointStateOptions options_;
    mutable std::mutex mutex_;
    std::unordered_map<
        cluster::Endpoint,
        std::shared_ptr<EndpointState>,
        cluster::EndpointHash
    > states_;
    std::unordered_map<
        cluster::Endpoint,
        double,
        cluster::EndpointHash
    > weights_;
    EndpointSnapshot endpoint_snapshot_;
    StateSnapshot state_snapshot_;

    net::EventLoop* loop_=nullptr;
    ProbeFunction probe_;
    ActiveHealthCheckOptions active_options_;
    bool running_=false;
    std::uint64_t generation_=0;
    TimerId timer_id_=0;
};

HealthChecker::HealthChecker(
    EndpointStateOptions options
):impl_(std::make_shared<Impl>(options)){}

HealthChecker::~HealthChecker(){
    Stop();
}

HealthChecker::StateSnapshot HealthChecker::Update(
    const EndpointSnapshot& endpoints
){
    return impl_->Update(endpoints);
}

std::shared_ptr<EndpointState> HealthChecker::Find(
    const cluster::Endpoint& endpoint
)const{
    return impl_->Find(endpoint);
}

void HealthChecker::SetWeight(
    const cluster::Endpoint& endpoint,
    double weight
){
    impl_->SetWeight(endpoint,weight);
}

void HealthChecker::UpdateEndpointOptions(
    EndpointStateOptions options
){
    impl_->UpdateEndpointOptions(options);
}

void HealthChecker::UpdateActiveOptions(
    ActiveHealthCheckOptions options
){
    impl_->UpdateActiveOptions(options);
}

std::size_t HealthChecker::Size()const{
    return impl_->Size();
}

void HealthChecker::Start(
    net::EventLoop* loop,
    ProbeFunction probe,
    ActiveHealthCheckOptions options
){
    impl_->Start(loop,std::move(probe),options);
}

void HealthChecker::Stop()noexcept{
    impl_->Stop();
}

bool HealthChecker::IsRunning()const noexcept{
    return impl_->IsRunning();
}

}
