#pragma once
#include <functional>
#include <cstdint>


namespace asterrpc::net
{
class EventLoop;


class Channel
{

public:

    using Callback=std::function<void()>;
    Channel(EventLoop*loop,int fd);
    ~Channel() = default;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;
    Channel(Channel&&) = delete;
    Channel& operator=(Channel&&) = delete;

    void HandleEvent();

    int Fd()const noexcept;
    uint32_t Events()const noexcept;
    uint32_t Revents() const noexcept;
    void SetRevents(uint32_t revents) noexcept;

    void EnableReading();
    void EnableWriting();
    void DisableWriting();
    void DisableAll();

    void SetReadCallback(Callback cb);
    void SetWriteCallback(Callback cb);
    void SetCloseCallback(Callback cb);
    void SetErrorCallback(Callback cb);

    bool IsInEpoll()const noexcept;
    void SetInEpoll(bool inepoll)noexcept;
private:
    void Update();
private:
    EventLoop *loop_;
    int fd_;
    bool inepoll_;

    uint32_t events_;   //希望监听的事件
    uint32_t revents_;      //实际发生的事件

    Callback read_callback_;
    Callback write_callback_;
    Callback close_callback_;
    Callback error_callback_;
};

} // namespace asterrpc::net


