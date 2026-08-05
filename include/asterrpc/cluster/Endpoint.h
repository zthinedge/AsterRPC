#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace asterrpc::cluster{

class Endpoint{
public:
    Endpoint(std::string ip,std::uint16_t port);

    const std::string& Ip()const noexcept;
    std::uint16_t Port()const noexcept;
    std::string ToString()const;

    bool operator==(const Endpoint& other)const noexcept;
    bool operator!=(const Endpoint& other)const noexcept;

private:
    std::string ip_;
    std::uint16_t port_;
};

struct EndpointHash{
    std::size_t operator()(const Endpoint& endpoint)const noexcept;
};

}
