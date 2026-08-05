# AsterRPC 网络层设计

## 1. 网络层职责

网络层负责客户端与服务端之间的 TCP 通信。接收时，它把 TCP 字节追加到输入
`Buffer`，再通过消息回调交给协议层；发送时，它接收协议层编码后的字节，并通过
非阻塞 `send` 写入内核。

网络层不理解 `RpcMessage`、服务名或方法名，只处理连接、字节流和 IO 事件。

## 2. 组件关系

```text
                         TcpServer
                            |
                  +---------+----------+
                  |                    |
            main EventLoop       EventLoopThreadPool
                  |                    |
              Acceptor        sub EventLoop 0..N-1
                  |                    |
             listen fd          TcpConnection
                                      |
                              Socket + Channel
                              input/output Buffer
```

- `Socket` 使用 RAII 管理 fd，封装 bind、listen、accept、connect、send 和 recv。
- `Channel` 描述一个 fd 关注的事件和回调，但不拥有 fd。
- `Poller` 封装 epoll，返回本轮已就绪的 `Channel`。
- `EventLoop` 驱动 Poller、事件回调和跨线程待执行任务。
- `Acceptor` 只负责监听和接收新连接。
- `TcpConnection` 管理一条已建立连接及其收发缓冲区。
- `TcpServer` 管理 Acceptor、IO 线程池和所有服务端连接。

## 3. 主从 Reactor

主 Reactor 运行在调用 `RpcServer::Start()` 的线程中，只监听 listen fd。新连接到达
后，`Acceptor` 完成 accept，`TcpServer` 再把该连接分配给一个 sub Reactor。

sub Reactor 各自运行在独立的 `EventLoopThread` 中，负责分配给自己的连接，包括：

- 监听连接 fd 的可读、可写、错误和关闭事件；
- 执行 `TcpConnection` 的读写回调；
- 管理输入、输出 Buffer；
- 执行投递到本 EventLoop 的 Functor。

当 `io_threads` 为 0 时，不创建 sub Reactor，所有连接继续由 main EventLoop
处理，便于运行单 Reactor 基线和兼容原来的使用方式。

## 4. IO 连接负载均衡

`EventLoopThreadPool` 支持两种连接分配策略：

- `RoundRobin`：按顺序轮询 sub Reactor，实现简单，分配开销固定。
- `LeastConnections`：选择当前活动连接数最少的 sub Reactor，连接生命周期差异较大
  时更均衡。

这里均衡的是“连接归属”，不是对每个 RPC 请求重新选择线程。一条 TCP 连接在整个
生命周期内固定属于同一个 EventLoop，避免并发操作 Channel、Buffer 和 fd。

## 5. EventLoop 跨线程投递

每个 `EventLoop` 记录所属线程。其他线程不能直接操作它管理的 Channel，而应使用：

- `RunInLoop`：如果当前就在所属线程立即执行，否则加入任务队列；
- `QueueInLoop`：始终加入任务队列；
- `eventfd`：写入后唤醒阻塞于 `epoll_wait` 的 EventLoop。

待执行队列由互斥锁保护。EventLoop 会先将任务交换到局部容器，再逐个执行，避免
执行回调期间长期持锁。`Stop()` 也会写 eventfd，所以跨线程停止能够及时生效。

`TcpConnection::Send`、`Shutdown` 和 `Close` 已封装上述规则：从业务线程调用时，
操作会自动投递回该连接所属的 IO 线程。

## 6. 业务线程池

网络 IO 与 RPC 业务处理采用不同的线程：

```text
sub Reactor 收到字节
  -> RpcCodec 解码
  -> 请求提交到 ThreadPool
  -> ServiceDispatcher 调用业务 Handler
  -> TcpConnection::Send
  -> 投递回连接所属 sub Reactor
  -> 非阻塞发送响应
```

业务线程池采用有界任务队列，IO 线程通过 `TrySubmit` 非阻塞提交任务。队列满时立即
返回错误，避免 IO 线程因等待队列空间而失去响应能力。业务 Handler 会被并发调用，
因此 Handler 访问共享状态时必须自行保证线程安全。

`business_threads` 为 0 时，Handler 仍在 IO 线程执行。这适合极轻量的 Echo 基线；
对于 CPU 密集、阻塞或耗时业务，应配置业务线程池以隔离 IO 和业务执行。

## 7. TcpConnection 读写流程

可读事件发生后，`HandleRead` 循环 recv，直到返回 `EAGAIN`，把数据追加到输入
Buffer，再调用消息回调。一次 recv 不保证得到一个完整 RPC 包，协议层会保留不完整
数据，等待后续可读事件。

发送时，如果输出 Buffer 没有积压，先尝试直接 send，减少一次内存拷贝；未发送完的
部分加入输出 Buffer，并监听可写事件。fd 再次可写时，`HandleWrite` 继续发送，全部
发送完成后取消可写事件。

## 8. 生命周期约束

- EventLoop 必须比注册在其中的 Channel 活得更久。
- Socket 和 Poller 使用 RAII 关闭 fd。
- TcpConnection 使用 `shared_ptr` 管理异步回调期间的生命周期。
- 跨线程任务捕获连接的 `weak_ptr`，执行前检查连接是否仍然存在。
- 关闭连接后，TcpServer 在 main EventLoop 中移除连接，并更新对应 sub Reactor
  的连接计数。

## 9. 配置示例

```cpp
asterrpc::rpc::RpcServerOptions options;
options.tcp.io_threads = 4;
options.tcp.io_load_balance =
    asterrpc::net::IoLoopLoadBalance::LeastConnections;
options.business_threads = 4;
options.business_queue_capacity = 65536;

asterrpc::rpc::RpcServer server(&loop, address, options);
```

示例服务端也支持等价的命令行参数：

```bash
./calculator_server 9000 \
  --io-threads 4 \
  --business-threads 4 \
  --business-queue 65536 \
  --io-balance least-connections
```

## 10. 测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

测试覆盖 EventLoop 跨线程唤醒、IO 线程分配策略、业务线程池队列、跨线程响应发送，
以及响应乱序时根据 `request_id` 正确匹配。

## 11. 当前限制

- main Reactor 仍由调用方线程运行，未封装为独立线程。
- IO 负载只依据活动连接数，不依据每条连接的实时流量或 EventLoop CPU 使用率。
- 业务任务队列满目前映射为通用 `InternalError`，后续可增加专门的过载错误码。
- 同机压测时，多个客户端 EventLoop 会和服务端线程竞争 CPU；测绝对上限时应使用
  独立压测机。
