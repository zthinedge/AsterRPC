#include "asterrpc/net/Socket.h"
#include "asterrpc/net/InetAddress.h"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>

using asterrpc::net::InetAddress;
using asterrpc::net::Socket;

int main() {
    std::promise<std::uint16_t> port_promise;
    std::future<std::uint16_t> port_future = port_promise.get_future();
    std::exception_ptr server_exception = nullptr;

    std::thread server_thread([&]() {
        try {
            Socket listen_socket;
            listen_socket.Bind(InetAddress("127.0.0.1", 0));
            listen_socket.Listen();

            sockaddr_in local_addr{};
            socklen_t local_len = sizeof(local_addr);
            if (::getsockname(
                    listen_socket.GetFd(),
                    reinterpret_cast<sockaddr*>(&local_addr),
                    &local_len) == -1) {
                throw std::runtime_error("getsockname() failed");
            }

            std::uint16_t port = ntohs(local_addr.sin_port);
            port_promise.set_value(port);

            Socket conn = listen_socket.Accept();

            char buffer[64]{};
            ssize_t n = conn.Recv(buffer, sizeof(buffer));
            std::string request(buffer, static_cast<std::size_t>(n));

            if (request != "ping") {
                throw std::runtime_error("server expected ping, got: " + request);
            }

            const char* response = "pong";
            conn.Send(response, std::strlen(response));
        } catch (...) {
            server_exception = std::current_exception();

            try {
                port_promise.set_exception(server_exception);
            } catch (const std::future_error&) {
            }
        }
    });

    std::uint16_t port = port_future.get();

    Socket client_socket;
    client_socket.Connect(InetAddress("127.0.0.1", port));

    const char* request = "ping";
    client_socket.Send(request, std::strlen(request));

    char buffer[64]{};
    ssize_t n = client_socket.Recv(buffer, sizeof(buffer));
    std::string response(buffer, static_cast<std::size_t>(n));

    server_thread.join();

    if (server_exception) {
        std::rethrow_exception(server_exception);
    }

    if (response != "pong") {
        throw std::runtime_error("client expected pong, got: " + response);
    }

    std::cout << "Socket test passed\n";
    return 0;
}
