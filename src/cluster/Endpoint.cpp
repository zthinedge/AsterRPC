#include "asterrpc/cluster/Endpoint.h"

#include <arpa/inet.h>
#include <functional>
#include <stdexcept>
#include <utility>

namespace asterrpc::cluster{

Endpoint::Endpoint(std::string ip,std::uint16_t port)
    :ip_(std::move(ip)),port_(port){
    in_addr address{};
    if(::inet_pton(AF_INET,ip_.c_str(),&address)!=1){
        throw std::invalid_argument("endpoint IP address is invalid");
    }
    if(port_==0){
        throw std::invalid_argument("endpoint port must not be zero");
    }
}

const std::string& Endpoint::Ip()const noexcept{
    return ip_;
}

std::uint16_t Endpoint::Port()const noexcept{
    return port_;
}

std::string Endpoint::ToString()const{
    return ip_+':'+std::to_string(port_);
}

bool Endpoint::operator==(const Endpoint& other)const noexcept{
    return ip_==other.ip_&&port_==other.port_;
}

bool Endpoint::operator!=(const Endpoint& other)const noexcept{
    return !(*this==other);
}

std::size_t EndpointHash::operator()(
    const Endpoint& endpoint
)const noexcept{
    std::size_t ip_hash=std::hash<std::string>{}(endpoint.Ip());
    std::size_t port_hash=
        std::hash<std::uint16_t>{}(endpoint.Port());
    return ip_hash^(port_hash+0x9e3779b9+(ip_hash<<6)+(ip_hash>>2));
}

}
