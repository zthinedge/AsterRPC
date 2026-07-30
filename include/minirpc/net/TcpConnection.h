#pragma once

#include "minirpc/net/Buffer.h"
#include "minirpc/net/Channel.h"
#include "minirpc/net/Socket.h"

#include <functional>
#include <memory>
#include <string>

namespace minirpc::net{

class EventLoop;

class TcpConnection:
    public std::enable_shared_from_this<TcpConnection>{
public:
    using MessageCallback=
        std::function<void(TcpConnection*,Buffer*)>;

    using CloseCallback=
        std::function<void(TcpConnection*)>;

    TcpConnection(EventLoop*loop,Socket socket);
    ~TcpConnection();

    void Start();
    void Send(const std::string& data);
    void Shutdown();
    void Close();

    void SetMessageCallback(MessageCallback cb);
    void SetCloseCallback(CloseCallback cb);

    int Fd()const noexcept;
    EventLoop* OwnerLoop()const noexcept;
    std::weak_ptr<TcpConnection> WeakFromThis()noexcept;
    std::weak_ptr<const int> LifetimeToken()const noexcept;

private:
    void StartInLoop();
    void SendInLoop(const std::string& data);
    void ShutdownInLoop();
    void CloseInLoop();

    void HandleRead();
    void HandleWrite();
    void HandleClose();

    EventLoop* loop_;
    Socket socket_;
    Channel channel_;

    Buffer input_buffer_;
    Buffer output_buffer_;

    MessageCallback message_callback_;
    CloseCallback close_callback_;
    std::shared_ptr<const int> lifetime_token_;
    bool close_after_write_;
    bool closed_;
};

}
