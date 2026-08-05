#pragma once

#include "asterrpc/cluster/Endpoint.h"

#include <atomic>
#include <memory>
#include <optional>
#include <vector>

namespace asterrpc::cluster{

class RoundRobin{
public:
    using EndpointSnapshot=
        std::shared_ptr<const std::vector<Endpoint>>;

    std::optional<Endpoint> Select(
        const EndpointSnapshot& endpoints
    );

private:
    std::atomic_uint64_t next_{0};
};

}
