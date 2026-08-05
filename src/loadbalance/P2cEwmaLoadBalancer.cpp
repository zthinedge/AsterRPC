#include "asterrpc/loadbalance/P2cEwmaLoadBalancer.h"

#include <algorithm>
#include <cstddef>
#include <utility>

namespace asterrpc::loadbalance{

P2cEwmaLoadBalancer::Selection::Selection(
    std::shared_ptr<health::EndpointState> state,
    health::RequestKind kind
):endpoint_(state->GetEndpoint()),
  state_(std::move(state)),
  kind_(kind){}

P2cEwmaLoadBalancer::Selection::Selection(
    Selection&& other
)noexcept
    :endpoint_(std::move(other.endpoint_)),
     state_(std::move(other.state_)),
     kind_(other.kind_){}

P2cEwmaLoadBalancer::Selection&
P2cEwmaLoadBalancer::Selection::operator=(
    Selection&& other
)noexcept{
    if(this==&other){
        return *this;
    }

    Cancel();
    endpoint_=std::move(other.endpoint_);
    state_=std::move(other.state_);
    kind_=other.kind_;
    return *this;
}

P2cEwmaLoadBalancer::Selection::~Selection(){
    Cancel();
}

const cluster::Endpoint&
P2cEwmaLoadBalancer::Selection::GetEndpoint()const noexcept{
    return endpoint_;
}

void P2cEwmaLoadBalancer::Selection::CompleteSuccess(
    std::chrono::microseconds latency
){
    if(state_==nullptr){
        return;
    }

    state_->CompleteSuccess(latency,kind_);
    state_.reset();
}

void P2cEwmaLoadBalancer::Selection::CompleteFailure(
    std::chrono::microseconds latency
){
    if(state_==nullptr){
        return;
    }

    state_->CompleteFailure(latency,kind_);
    state_.reset();
}

void P2cEwmaLoadBalancer::Selection::Cancel()noexcept{
    if(state_==nullptr){
        return;
    }

    state_->Cancel(kind_);
    state_.reset();
}

P2cEwmaLoadBalancer::P2cEwmaLoadBalancer(
    health::EndpointStateOptions options
):P2cEwmaLoadBalancer(
    options,
    std::random_device{}()
){}

P2cEwmaLoadBalancer::P2cEwmaLoadBalancer(
    health::EndpointStateOptions options,
    std::uint64_t random_seed
):health_checker_(options),random_(random_seed){}

std::optional<P2cEwmaLoadBalancer::Selection>
P2cEwmaLoadBalancer::Select(
    const EndpointSnapshot& endpoints
){
    auto states=health_checker_.Update(endpoints);
    std::vector<StatePtr> selectable;
    selectable.reserve(states->size());

    for(const auto& state:*states){
        if(state->Snapshot().selectable){
            selectable.push_back(state);
        }
    }

    while(!selectable.empty()){
        if(selectable.size()==1){
            return Acquire(selectable.front());
        }

        auto candidates=PickTwo(selectable);
        StatePtr first=candidates.first;
        StatePtr second=candidates.second;
        auto first_snapshot=first->Snapshot();
        auto second_snapshot=second->Snapshot();

        bool prefer_second=
            (!second_snapshot.has_latency_sample&&
             first_snapshot.has_latency_sample)||
            (second_snapshot.has_latency_sample==
                 first_snapshot.has_latency_sample&&
             second_snapshot.score<first_snapshot.score);
        if(prefer_second){
            std::swap(first,second);
        }

        auto selected=Acquire(first);
        if(selected.has_value()){
            return selected;
        }

        selected=Acquire(second);
        if(selected.has_value()){
            return selected;
        }

        selectable.erase(
            std::remove_if(
                selectable.begin(),
                selectable.end(),
                [](const StatePtr& state){
                    return !state->Snapshot().selectable;
                }
            ),
            selectable.end()
        );
    }

    return std::nullopt;
}

void P2cEwmaLoadBalancer::SetWeight(
    const cluster::Endpoint& endpoint,
    double weight
){
    health_checker_.SetWeight(endpoint,weight);
}

void P2cEwmaLoadBalancer::UpdateOptions(
    health::EndpointStateOptions options
){
    health_checker_.UpdateEndpointOptions(options);
}

health::HealthChecker&
P2cEwmaLoadBalancer::GetHealthChecker()noexcept{
    return health_checker_;
}

const health::HealthChecker&
P2cEwmaLoadBalancer::GetHealthChecker()const noexcept{
    return health_checker_;
}

std::optional<P2cEwmaLoadBalancer::Selection>
P2cEwmaLoadBalancer::Acquire(const StatePtr& state){
    auto kind=state->TryAcquire();
    if(!kind.has_value()){
        return std::nullopt;
    }

    return Selection(state,*kind);
}

std::pair<
    P2cEwmaLoadBalancer::StatePtr,
    P2cEwmaLoadBalancer::StatePtr
> P2cEwmaLoadBalancer::PickTwo(
    const std::vector<StatePtr>& states
){
    std::lock_guard<std::mutex> lock(random_mutex_);
    std::uniform_int_distribution<std::size_t> first_distribution(
        0,
        states.size()-1
    );
    std::size_t first=first_distribution(random_);

    std::uniform_int_distribution<std::size_t> second_distribution(
        0,
        states.size()-2
    );
    std::size_t second=second_distribution(random_);
    if(second>=first){
        ++second;
    }

    return {states[first],states[second]};
}

}
