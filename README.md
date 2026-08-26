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

## 从零配置环境

以下流程以 Ubuntu 22.04/24.04 为例。项目依赖 Linux 的 `epoll`、`eventfd` 和
`timerfd`，因此需要在 Linux 或 Linux 虚拟机中构建。

### 1. 获取代码

```bash
git clone https://github.com/zthinedge/AsterRPC.git
cd AsterRPC
```

如果已经下载仓库，只需进入项目根目录：

```bash
cd /home/zym/Share/AsterRPC
```

### 2. 安装构建依赖

推荐直接安装系统 Protobuf，这样首次构建和 CI 都不需要从源码编译整个
Protobuf：

```bash
sudo apt-get update
sudo apt-get install --yes \
  build-essential \
  cmake \
  git \
  libprotobuf-dev \
  protobuf-compiler
```

检查工具是否可用：

```bash
c++ --version
cmake --version
protoc --version
```

如果没有安装系统 Protobuf，CMake 会通过 `FetchContent` 自动从 GitHub 下载并
编译 Protobuf v35.0 及其依赖。这种方式需要正常访问 GitHub，第一次配置和编译
会明显更慢。

### 3. 配置和编译

Release 构建用于运行示例和性能测试：

```bash
cmake -S . -B build-simple -DCMAKE_BUILD_TYPE=Release
cmake --build build-simple -j"$(nproc)"
```

参数含义：

- `-S .`：源码目录是当前目录。
- `-B build-simple`：生成文件和编译产物放入 `build-simple/`。
- `CMAKE_BUILD_TYPE=Release`：启用优化，适合性能测试。
- `-j"$(nproc)"`：按照当前 CPU 数量并行编译。

配置成功时会看到：

```text
-- Configuring done
-- Generating done
-- Build files have been written to: .../build-simple
```

### 4. 运行自动化测试

```bash
ctest --test-dir build-simple --output-on-failure
```

正常结果应为：

```text
100% tests passed, 0 tests failed out of 9
```

开发和排错时也可以使用独立的 Debug 构建目录：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j"$(nproc)"
ctest --test-dir build-debug --output-on-failure
```

不要在同一个构建目录中频繁切换 Debug 和 Release。不同配置使用不同目录，能
避免旧缓存影响结果。

### 5. 主要构建产物

构建完成后，常用程序位于构建目录：

```text
build-simple/calculator_server   Calculator 与 Echo RPC 服务端
build-simple/calculator_client   Calculator RPC 客户端
build-simple/rpc_benchmark       RPC 性能测试工具
```

### 常见问题

#### CMake 长时间停在配置阶段

如果没有安装系统 Protobuf，CMake 正在下载 Protobuf v35.0。可以显示依赖下载
过程：

```bash
cmake -S . -B build-simple \
  -DCMAKE_BUILD_TYPE=Release \
  -DFETCHCONTENT_QUIET=OFF
```

也可以安装 `libprotobuf-dev` 和 `protobuf-compiler` 后，换一个新的构建目录重新
配置。

#### 出现 `^C` 或 `exit code 143`

- `^C` 表示构建被用户按 `Ctrl+C` 中断。
- `143` 表示进程收到 `SIGTERM`，通常是 CI 被新提交取消或任务被外部终止。

它们不等同于 C++ 编译错误。真正的编译错误通常包含 `error:`、
`undefined reference` 或缺少头文件等信息。

#### 端口被占用

如果服务端提示 `bind() failed`，说明端口可能已被使用。可以换一个端口，并让
客户端或 Benchmark 使用相同端口，例如 `19000`。

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

## 持续集成

项目通过 [GitHub Actions](.github/workflows/ci.yml) 执行 CI。以下事件会触发：

- 向仓库 push 新提交。
- 创建或更新 Pull Request。
- 在 Actions 页面手动运行工作流。

CI 会在新的 Ubuntu 环境中依次执行：

```text
检出当前提交
  -> 安装构建依赖
  -> Debug 配置 CMake
  -> 并行编译
  -> 使用 CTest 运行全部测试
```

本地复现 CI 的命令：

```bash
cmake -S . -B cmake-build-ci -DCMAKE_BUILD_TYPE=Debug
cmake --build cmake-build-ci --parallel
ctest --test-dir cmake-build-ci --output-on-failure
```

工作流设置了 `cancel-in-progress: true`。同一分支连续 push 时，GitHub 会取消仍在
执行的旧任务，只保留最新提交的 CI；旧任务可能显示 `exit code 143`。

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
