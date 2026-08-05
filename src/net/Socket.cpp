#include "asterrpc/net/Socket.h"
#include "asterrpc/net/InetAddress.h"

#include <unistd.h>        // close
#include <sys/socket.h>    // socket, bind, listen, accept, connect
#include <arpa/inet.h>     // sockaddr_in
#include <cstring>         // strerror
#include <stdexcept>       // runtime_error
#include <cerrno>          // errno

namespace asterrpc::net {

// 构造：创建 TCP 套接字
Socket::Socket() : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {
    if (fd_ == -1) {
        throw std::runtime_error("socket() failed: " + std::string(strerror(errno)));
    }
}

Socket::~Socket() {
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}

// 移动构造
Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

// 移动赋值
Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        if (fd_ != -1) ::close(fd_);
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

// 私有构造（仅用于 Accept）
Socket::Socket(int fd) : fd_(fd) {}

// -------------------- 实现网络操作 --------------------

void Socket::Bind(const InetAddress& addr) {
    if (::bind(fd_, addr.GetSockAddr(), addr.GetSockLen()) == -1) {
        throw std::runtime_error("bind() failed: " + std::string(strerror(errno)));
    }
}

void Socket::Listen(int backlog) {
    if (::listen(fd_, backlog) == -1) {
        throw std::runtime_error("listen() failed: " + std::string(strerror(errno)));
    }
}

Socket Socket::Accept(InetAddress* peer_addr) {
    sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    int client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
    if (client_fd == -1) {
        throw std::runtime_error("accept() failed: " + std::string(strerror(errno)));
    }

    // 如果调用者传入了指针，就把客户端地址填进去（需要 InetAddress 支持从 sockaddr_in 构造）
    if (peer_addr != nullptr) {
        
        *peer_addr = InetAddress(client_addr);
    }

    // 调用私有构造函数，接管 client_fd
    return Socket(client_fd);
}

void Socket::Connect(const InetAddress& server_addr) {
    if (::connect(fd_, server_addr.GetSockAddr(), server_addr.GetSockLen()) == -1) {
        throw std::runtime_error("connect() failed: " + std::string(strerror(errno)));
    }
}

ssize_t Socket::Send(const void* data, size_t len, int flags) {
    ssize_t sent = ::send(fd_, data, len, flags);
    if (sent == -1) {
        throw std::runtime_error("send() failed: " + std::string(strerror(errno)));
    }
    return sent;
}

ssize_t Socket::Recv(void* buffer, size_t len, int flags) {
    ssize_t recvd = ::recv(fd_, buffer, len, flags);
    if (recvd == -1) {
        throw std::runtime_error("recv() failed: " + std::string(strerror(errno)));
    }
    return recvd; // 返回 0 表示对端关闭
}

int Socket::GetFd() const noexcept {
    return fd_;
}

//端口重用
void Socket::SetReuseAddr(bool on){
    int opt=on?1:0;

    if(::setsockopt(fd_,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt))==-1){
        throw std::runtime_error("setsockopt(SO_REUSEADDR) failed: "+std::string(strerror(errno)));
    }
}
//非阻塞
void Socket::SetNonBlocking(){
    int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags == -1) {
        throw std::runtime_error("fcntl(F_GETFL) failed: " +
                                 std::string(strerror(errno)));
    }
    //在原来的属性上加O_NONBLOCK
    if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
        throw std::runtime_error("fcntl(F_SETFL) failed: " +
                                 std::string(strerror(errno)));
    }
}


} // namespace net::asterrpc
