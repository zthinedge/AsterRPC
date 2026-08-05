#include "asterrpc/net/InetAddress.h"
#include <stdexcept>

namespace asterrpc::net {

InetAddress::InetAddress(const std::string& ip, std::uint16_t port) {
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(port);

    int res = inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
    if (res != 1) {
        throw std::invalid_argument("invalid ip address");
    }
}

InetAddress::InetAddress(const sockaddr_in& addr)
    : addr_(addr) {}

std::string InetAddress::Ip() const {
    char buf[INET_ADDRSTRLEN];
    const char* result = inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
    return result ? std::string(buf) : std::string();
}

std::uint16_t InetAddress::Port() const {
    return ntohs(addr_.sin_port);
}

}