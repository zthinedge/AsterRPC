#pragma once
#include <vector>
#include <sys/epoll.h>
namespace asterrpc::net{
class Channel;
class EventLoop;

class Poller{
public:
    using ChannelList=std::vector<Channel*>;
    explicit Poller(EventLoop*loop);
    ~Poller();
    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;


    void Poll(int timeout_ms,ChannelList*chs);
    void UpdateChannel(Channel*ch);
    void RemoveChannel(Channel*ch);
private:
    EventLoop *loop_;
    int epoll_fd_;
    std::vector<epoll_event>events_;
};

}
