#include "minirpc/health/HealthChecker.h"

#include <cmath>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace minirpc::health{

HealthChecker::HealthChecker(
    EndpointStateOptions options
):options_(options),
  state_snapshot_(std::make_shared<const StateList>()){}

HealthChecker::StateSnapshot HealthChecker::Update(
    const EndpointSnapshot& endpoints
){
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
                std::make_shared<EndpointState>(endpoint,options)
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

std::shared_ptr<EndpointState> HealthChecker::Find(
    const cluster::Endpoint& endpoint
)const{
    std::lock_guard<std::mutex> lock(mutex_);
    auto state=states_.find(endpoint);
    return state==states_.end()?nullptr:state->second;
}

void HealthChecker::SetWeight(
    const cluster::Endpoint& endpoint,
    double weight
){
    if(!std::isfinite(weight)||weight<=0.0){
        throw std::invalid_argument("endpoint weight must be positive");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto state=states_.find(endpoint);

    if(state!=states_.end()){
        state->second->SetWeight(weight);
    }

    weights_[endpoint]=weight;
}

std::size_t HealthChecker::Size()const{
    std::lock_guard<std::mutex> lock(mutex_);
    return states_.size();
}

}
