#include "minirpc/net/TcpClient.h"
#include "minirpc/net/EventLoop.h"

#include <utility>

namespace minirpc::net{

TcpClient::TcpClient(EventLoop*loop,const InetAddress& server_addr)
    :loop_(loop),connector_(loop,server_addr){
    connector_.SetNewConnectionCallback([this](Socket socket){
        HandleNewConnection(std::move(socket));
    });

    connector_.SetErrorCallback([this](int error){
        if(error_callback_){
            error_callback_(error);
        }
    });
}

void TcpClient::Connect(){
    connector_.Connect();
}

void TcpClient::Disconnect(){
    loop_->RunInLoop([this](){
        std::shared_ptr<TcpConnection> connection=
            std::atomic_load(&connection_);
        if(connection){
            connection->Close();
        }
    });
}

void TcpClient::Send(const std::string& data){
    std::shared_ptr<TcpConnection> connection=
        std::atomic_load(&connection_);
    if(connection){
        connection->Send(data);
    }
}

bool TcpClient::IsConnected()const noexcept{
    return std::atomic_load(&connection_)!=nullptr;
}

void TcpClient::SetConnectionCallback(ConnectionCallback cb){
    connection_callback_=std::move(cb);
}

void TcpClient::SetCloseCallback(CloseCallback cb){
    close_callback_=std::move(cb);
}

void TcpClient::SetMessageCallback(MessageCallback cb){
    message_callback_=std::move(cb);
}

void TcpClient::SetErrorCallback(ErrorCallback cb){
    error_callback_=std::move(cb);
}

void TcpClient::HandleNewConnection(Socket socket){
    auto connection=std::make_shared<TcpConnection>(
        loop_,
        std::move(socket)
    );

    connection->SetMessageCallback(
        [this](TcpConnection* connection,Buffer* buffer){
            if(message_callback_){
                message_callback_(connection,buffer);
            }
        }
    );

    connection->SetCloseCallback(
        [this](TcpConnection* connection){
            HandleClose(connection);
        }
    );

    std::atomic_store(&connection_,connection);
    connection->Start();

    if(connection_callback_){
        connection_callback_(connection.get());
    }
}

void TcpClient::HandleClose(TcpConnection* connection){
    std::shared_ptr<TcpConnection> current=
        std::atomic_load(&connection_);
    if(current.get()!=connection){
        return;
    }

    EventLoop* loop=loop_;
    CloseCallback callback=close_callback_;
    std::atomic_store(
        &connection_,
        std::shared_ptr<TcpConnection>{}
    );

    // TcpConnection 正在执行自己的关闭回调，不能在回调栈中析构。
    // 延迟任务只持有连接，不捕获 TcpClient 的裸 this。
    loop->QueueInLoop([current=std::move(current)](){});

    if(callback){
        callback();
    }
}

}
