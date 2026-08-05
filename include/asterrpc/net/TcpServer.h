#pragma once

#include "asterrpc/net/Acceptor.h"
#include "asterrpc/net/EventLoopThreadPool.h"
#include "asterrpc/net/TcpConnection.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace asterrpc::net{

class EventLoop;
class InetAddress;

struct TcpServerOptions{
    std::size_t io_threads=0;
    IoLoopLoadBalance io_load_balance=
        IoLoopLoadBalance::RoundRobin;
};

class TcpServer{
public:
    using MessageCallback=TcpConnection::MessageCallback;
    using ConnectionCallback=std::function<void()>;
    using CloseCallback=std::function<void()>;

    TcpServer(
        EventLoop* loop,
        const InetAddress& addr,
        TcpServerOptions options={}
    );
    ~TcpServer();

    void Start();
    void SetMessageCallback(MessageCallback cb);
    void SetConnectionCallback(ConnectionCallback cb);
    void SetCloseCallback(CloseCallback cb);

    std::size_t IoThreadCount()const noexcept;
    std::vector<std::size_t> IoConnectionCounts()const;

private:
    struct ConnectionEntry{
        std::shared_ptr<TcpConnection> connection;
        std::size_t worker_index=
            EventLoopThreadPool::kBaseLoopIndex;
    };

    void HandleNewConnection(Socket socket,const InetAddress& peer_addr);

    void HandleClose(TcpConnection* connection);
    void CloseAllConnections()noexcept;

    EventLoop* loop_;
    Acceptor acceptor_;
    EventLoopThreadPool io_pool_;

    std::unordered_map<int,ConnectionEntry> connections_;

    MessageCallback message_callback_;
    ConnectionCallback connection_callback_;
    CloseCallback close_callback_;
};

}
