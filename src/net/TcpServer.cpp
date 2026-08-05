#include "asterrpc/net/TcpServer.h"
#include "asterrpc/net/EventLoop.h"

#include <future>
#include <utility>
#include <vector>

namespace asterrpc::net{

TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& addr,
    TcpServerOptions options
):loop_(loop),
  acceptor_(loop,addr),
  io_pool_(
      loop,
      EventLoopThreadPoolOptions{
          options.io_threads,
          options.io_load_balance
      }
  ){
    acceptor_.SetNewConnectionCallback(
        [this](Socket socket,const InetAddress& peer_addr){
            HandleNewConnection(std::move(socket),peer_addr);
        }
    );
}

TcpServer::~TcpServer(){
    CloseAllConnections();
    io_pool_.Stop();
}

void TcpServer::Start(){
    io_pool_.Start();
    acceptor_.Listen();
}

void TcpServer::SetMessageCallback(MessageCallback cb){
    message_callback_=std::move(cb);
}

void TcpServer::SetConnectionCallback(ConnectionCallback cb){
    connection_callback_=std::move(cb);
}

void TcpServer::SetCloseCallback(CloseCallback cb){
    close_callback_=std::move(cb);
}

std::size_t TcpServer::IoThreadCount()const noexcept{
    return io_pool_.ThreadCount();
}

std::vector<std::size_t>
TcpServer::IoConnectionCounts()const{
    return io_pool_.ConnectionCounts();
}

void TcpServer::HandleNewConnection(Socket socket,const InetAddress&){
    EventLoopThreadPool::Selection selected=io_pool_.AcquireLoop();
    auto connection=std::make_shared<TcpConnection>(
        selected.loop,
        std::move(socket)
    );

    int fd=connection->Fd();

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

    TcpConnection* connection_ptr=connection.get();
    connections_.emplace(
        fd,
        ConnectionEntry{connection,selected.worker_index}
    );
    connection_ptr->Start();

    if(connection_callback_){
        connection_callback_();
    }
}

void TcpServer::HandleClose(TcpConnection* connection){
    int fd=connection->Fd();
    EventLoop* owner=connection->OwnerLoop();
    std::shared_ptr<TcpConnection> keep_alive=
        connection->WeakFromThis().lock();

    //放入EventLoop的待执行队列
    loop_->QueueInLoop([
        this,
        fd,
        owner,
        keep_alive=std::move(keep_alive)
    ]()mutable{
        auto entry=connections_.find(fd);
        if(entry!=connections_.end()){
            std::size_t worker_index=entry->second.worker_index;
            connections_.erase(entry);
            io_pool_.ReleaseLoop(worker_index);

            if(close_callback_){
                close_callback_();
            }
        }

        // 最后一个引用必须在连接所属的 EventLoop 中释放，
        // 避免在 main Reactor 线程析构 sub Reactor 的 Channel。
        owner->QueueInLoop([
            keep_alive=std::move(keep_alive)
        ](){});
    });
}

void TcpServer::CloseAllConnections()noexcept{
    std::vector<std::future<void>> closed;
    closed.reserve(connections_.size());

    for(auto& item:connections_){
        std::shared_ptr<TcpConnection> connection=
            std::move(item.second.connection);
        EventLoop* owner=connection->OwnerLoop();

        if(owner->IsInLoopThread()){
            connection->SetCloseCallback({});
            connection->Close();
            continue;
        }

        auto completion=std::make_shared<std::promise<void>>();
        closed.push_back(completion->get_future());
        owner->QueueInLoop([connection,completion](){
            connection->SetCloseCallback({});
            connection->Close();
            completion->set_value();
        });
    }

    for(auto& future:closed){
        try{
            future.get();
        }catch(...){
        }
    }

    connections_.clear();
}

}
