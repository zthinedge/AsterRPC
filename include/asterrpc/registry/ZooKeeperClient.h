#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace asterrpc::registry{

enum class ZooKeeperConnectionState{
    Stopped,
    Connecting,
    Connected,
    Disconnected,
    Expired,
    AuthFailed
};

struct ZooKeeperConnectionEvent{
    ZooKeeperConnectionState state=ZooKeeperConnectionState::Stopped;
    std::uint64_t generation=0;
};

struct ZooKeeperClientOptions{
    std::string servers="127.0.0.1:2181";
    std::chrono::milliseconds session_timeout{30000};
    std::chrono::milliseconds reconnect_delay{1000};
};

class ZooKeeperError:public std::runtime_error{
public:
    ZooKeeperError(std::string operation,int code);

    int Code()const noexcept;

private:
    int code_;
};

class ZooKeeperClient{
public:
    using ListenerId=std::uint64_t;
    using StateCallback=
        std::function<void(const ZooKeeperConnectionEvent&)>;
    using ChildrenWatchCallback=std::function<void()>;
    using DataWatchCallback=std::function<void()>;

    explicit ZooKeeperClient(ZooKeeperClientOptions options={});
    ~ZooKeeperClient();

    ZooKeeperClient(const ZooKeeperClient&)=delete;
    ZooKeeperClient& operator=(const ZooKeeperClient&)=delete;
    ZooKeeperClient(ZooKeeperClient&&)=delete;
    ZooKeeperClient& operator=(ZooKeeperClient&&)=delete;

    void Start();
    void Close()noexcept;

    bool IsConnected()const noexcept;
    ZooKeeperConnectionEvent CurrentEvent()const noexcept;
    bool WaitUntilConnected(std::chrono::milliseconds timeout);

    ListenerId AddStateListener(StateCallback callback);
    void RemoveStateListener(ListenerId listener_id)noexcept;

    void EnsurePersistentPath(const std::string& path);

    std::string CreateEphemeralSequential(
        const std::string& path_prefix,
        const std::string& data
    );

    std::vector<std::string> GetChildrenAndWatch(
        const std::string& path,
        ChildrenWatchCallback callback
    );

    std::string GetData(const std::string& path);
    std::string GetDataAndWatch(
        const std::string& path,
        DataWatchCallback callback
    );
    void SetData(
        const std::string& path,
        const std::string& data
    );

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
