#include "asterrpc/cluster/RoundRobin.h"

#include <cstddef>
#include <cstdint>

namespace asterrpc::cluster{

std::optional<Endpoint> RoundRobin::Select(
    const EndpointSnapshot& endpoints
){
    if(endpoints==nullptr||endpoints->empty()){
        return std::nullopt;
    }

    std::uint64_t sequence=next_.fetch_add(
        1,
        std::memory_order_relaxed
    );
    std::size_t index=static_cast<std::size_t>(
        sequence%endpoints->size()
    );
    return (*endpoints)[index];
}

}
