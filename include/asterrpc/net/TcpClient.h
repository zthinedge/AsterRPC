#pragma once

#include "asterrpc/net/Connector.h"
#include "asterrpc/net/TcpConnection.h"

#include <functional>
#include <memory>
#include <string>

namespace asterrpc::net{

class EventLoop;
class InetAddress;

class TcpClient{
public:
    using ConnectionCallback=std::function<void(TcpConnection*)>;
    using CloseCallback=std::function<void()>;
    using MessageCallback=TcpConnection::MessageCallback;
    using ErrorCallback=Connector::ErrorCallback;

    TcpClient(EventLoop*loop,const InetAddress& server_addr);
    ~TcpClient()=default;

    TcpClient(const TcpClient&)=delete;
    TcpClient& operator=(const TcpClient&)=delete;
    TcpClient(TcpClient&&)=delete;
    TcpClient& operator=(TcpClient&&)=delete;

    void Connect();
    void Disconnect();
    void Send(const std::string& data);

    bool IsConnected()const noexcept;

    void SetConnectionCallback(ConnectionCallback cb);
    void SetCloseCallback(CloseCallback cb);
    void SetMessageCallback(MessageCallback cb);
    void SetErrorCallback(ErrorCallback cb);

private:
    void HandleNewConnection(Socket socket);
    void HandleClose(TcpConnection* connection);

    EventLoop* loop_;
    Connector connector_;
    std::shared_ptr<TcpConnection> connection_;

    ConnectionCallback connection_callback_;
    CloseCallback close_callback_;
    MessageCallback message_callback_;
    ErrorCallback error_callback_;
};

}
