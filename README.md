# AsterRPC

AsterRPC 是一个基于 C++17 和 Reactor 网络模型实现的轻量级 Unary RPC 框架。
这个分支只保留网络、协议和 RPC 调用主链路，客户端直接连接明确的服务端地址，
不包含注册中心、集群治理、负载均衡、链路追踪和 HTTP 网关。

## 核心能力

- 基于 `epoll` 的主从 Reactor，支持多 IO 线程和 `eventfd` 跨线程唤醒。
- 基于 `timerfd` 的 EventLoop 定时任务，用于 RPC Deadline。
- 固定 24 字节 Header + Meta + Payload 的二进制协议。
- 增量解码，正确处理 TCP 半包、粘包和非法帧。
- 基于 `request_id -> PendingCall` 的单连接并发复用和乱序响应匹配。
- 同步、Future、Callback 三种调用接口。
- 有界业务线程池隔离网络 IO 和方法执行。
- Protobuf 业务对象序列化示例。

## 模块

```text
include/asterrpc/net       Reactor、连接、Buffer、定时器
include/asterrpc/protocol  RPC Header、Meta、Message、Codec
include/asterrpc/rpc       Client、Server、PendingCalls、Dispatcher
include/asterrpc/common    有界业务线程池
examples/services          Calculator Protobuf 与 Stub/Adapter
examples/client            Calculator 客户端
examples/server            Calculator 服务端
tests                      网络、协议和 RPC 核心测试
```

## 构建与测试

```bash
cmake -S . -B build-simple -DCMAKE_BUILD_TYPE=Release
cmake --build build-simple -j"$(nproc)"
ctest --test-dir build-simple --output-on-failure
```

## 运行 Calculator

服务端：

```bash
./build-simple/calculator_server 9000 \
  --io-threads 4 \
  --business-threads 4 \
  --business-queue 65536
```

客户端：

```bash
./build-simple/calculator_client 127.0.0.1 9000
```

连接成功后可以重复输入两个整数，每行发起一次 `Add` RPC；输入 `q` 退出：

```text
> 20 22
20 + 22 = 42
> 100 -7
100 + -7 = 93
> q
```

也可以使用单次调用模式：

```bash
./build-simple/calculator_client 127.0.0.1 9000 20 22
```

## RPC Benchmark

服务端还注册了用于压测的 `BenchService.Echo` 方法。启动服务端后，
可以用一个压测进程创建多条 TCP 连接，并在每条连接上复用多个并发请求：

```bash
./build-simple/rpc_benchmark \
  --host 127.0.0.1 \
  --port 9000 \
  --connections 4 \
  --concurrency 200 \
  --requests 100000 \
  --payload-bytes 1024 \
  --deadline-ms 500
```

其中 `connections` 是 TCP 连接数，`concurrency` 是所有连接合计的
未完成 RPC 数量，不代表线程数。单连接并发复用可以这样验证：

```bash
./build-simple/rpc_benchmark \
  --connections 1 \
  --concurrency 200 \
  --requests 100000
```

需要让脚本或测试助手读取结果时，增加 `--json`：

```bash
./build-simple/rpc_benchmark \
  --connections 4 \
  --concurrency 200 \
  --requests 100000 \
  --json
```

`connections`、`concurrency` 的准确含义、推荐测试矩阵、测试环境和基线结果见
[Benchmark 文档](docs/BENCHMARK.md)。

## 一次调用的主链路

```text
Protobuf Request
  -> Stub Serialize
  -> RpcClient
  -> request_id / PendingCall / Deadline Timer
  -> RpcCodec Encode
  -> TcpConnection
  -> RpcServer Decode
  -> business ThreadPool
  -> ServiceDispatcher
  -> Handler Parse / Invoke / Serialize
  -> Response with the same request_id
  -> PendingCalls Complete
  -> Future or Callback
```
