#pragma once

#include "asterrpc/net/Channel.h"
#include "asterrpc/net/InetAddress.h"
#include "asterrpc/net/Socket.h"

#include <functional>

namespace asterrpc::net{

class EventLoop;

class Connector{
public:
    using NewConnectionCallback=std::function<void(Socket)>;
    using ErrorCallback=std::function<void(int)>;

    Connector(EventLoop*loop,const InetAddress& server_addr);
    ~Connector();

    Connector(const Connector&)=delete;
    Connector& operator=(const Connector&)=delete;
    Connector(Connector&&)=delete;
    Connector& operator=(Connector&&)=delete;

    void SetNewConnectionCallback(NewConnectionCallback cb);
    void SetErrorCallback(ErrorCallback cb);

    void Connect();

private:
    enum class State{
        Disconnected,
        Connecting,
        Connected,
        Failed
    };

    void HandleWrite();
    void HandleError();
    int GetSocketError()const noexcept;
    void NotifyConnected();
    void NotifyError(int error);

    InetAddress server_addr_;
    Socket socket_;
    Channel channel_;
    State state_;

    NewConnectionCallback new_connection_callback_;
    ErrorCallback error_callback_;
};

}
