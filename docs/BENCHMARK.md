# AsterRPC 性能基线

## 测试对象

Benchmark 调用服务端注册的 `BenchService.Echo` 方法。服务端收到 Payload 后原样
返回，用于测量 AsterRPC 的网络传输、协议编解码、请求匹配和线程调度开销，
不代表复杂业务逻辑的性能。

主要指标：

- `QPS`：每秒完成的 RPC 数量。
- `P50`：50% 的成功请求延迟不超过该值。
- `P99`：99% 的成功请求延迟不超过该值，用来观察尾延迟。
- `failed/timeouts`：失败和超时请求数，正式基线应为 0。

## 参数含义

### connections

`connections` 是压测客户端创建的 TCP 连接数，也是 `RpcClient` 实例数。

它不等于线程数。当前 `rpc_benchmark` 中的所有 `RpcClient` 共用一个客户端
`EventLoop`，由一个 EventLoop 线程管理。服务端的 IO 线程数量由启动参数
`--io-threads` 单独决定。

例如：

```text
connections = 4

一个压测进程
  -> RpcClient 0 -> TCP connection 0
  -> RpcClient 1 -> TCP connection 1
  -> RpcClient 2 -> TCP connection 2
  -> RpcClient 3 -> TCP connection 3

四个 RpcClient 共用同一个客户端 EventLoop，不是四个客户端线程。
```

### concurrency

`concurrency` 是所有连接合计允许同时处于“已发送但尚未收到响应”状态的 RPC
数量，也叫总 in-flight 请求数。它不是线程数，也不是总请求数。

例如：

```text
connections = 1, concurrency = 100
```

表示一条 TCP 连接上最多同时挂起 100 个 RPC。每个请求拥有不同的
`request_id`，响应到达后由 `PendingCalls` 匹配到对应请求。这一场景主要验证
单连接并发复用能力。

```text
connections = 4, concurrency = 200
```

表示 4 条连接合计最多挂起 200 个 RPC。Benchmark 以轮询方式选择已连接的
客户端，稳定状态下平均每条连接约有 50 个未完成请求。

当一个请求完成时，Benchmark 会继续补发一个请求，直到完成 `requests` 指定的
总请求数。因此它采用的是固定并发（closed-loop）压测模型：

```text
先发满 concurrency 个请求
  -> 一个响应完成
  -> 补发一个新请求
  -> 始终维持目标并发
  -> 完成 requests 个请求后停止
```

该参数主要测试：

- 单连接能否并发复用多个 RPC。
- `request_id -> PendingCall` 能否正确匹配响应。
- 并发增加后吞吐量是否提升、P99 是否恶化。
- 多条连接能否利用服务端的多个 IO 线程。

## 构建与服务端配置

使用 Release 构建：

```bash
cmake -S . -B build-simple -DCMAKE_BUILD_TYPE=Release
cmake --build build-simple -j"$(nproc)"
```

启动服务端：

```bash
./build-simple/calculator_server 9000 \
  --io-threads 4 \
  --business-threads 4 \
  --business-queue 65536
```

以下测试均通过 `127.0.0.1` 在同一台虚拟机中运行客户端和服务端，测得的是
本机回环性能，不代表跨机器真实网络性能。

## Reactor 配置说明

`--io-threads` 表示 Sub Reactor 数量，不包含负责监听连接的 Main Reactor：

```text
--io-threads 0：单 Reactor；Main Reactor 同时 accept、收发和编解码
--io-threads 1：1 个 Main Reactor + 1 个 Sub Reactor
--io-threads 4：1 个 Main Reactor + 4 个 Sub Reactor
```

因此，将 `--io-threads` 设置为 1 并不是严格意义上的单 Reactor。测试纯单
Reactor 时应使用：

```bash
./build-simple/calculator_server 9000 \
  --io-threads 0 \
  --business-threads 0
```

这里同时关闭业务线程池，使 `BenchService.Echo` 直接在 Reactor 线程中执行，
用于观察网络收发、协议编解码和请求匹配本身的开销。Echo 只返回原 Payload，
不适合用来代表耗时业务在线程池中的表现。

单 Reactor 在低负载下可能更快，因为没有跨 EventLoop 投递、`eventfd` 唤醒和
线程切换开销；高连接数、高并发时，一个线程需要处理所有连接，吞吐量和尾延迟
可能逐渐受限。主从 Reactor 是否更快取决于 CPU 核数、连接数、并发量和业务
线程配置，需要通过相同参数实测，不能只根据线程数量判断。

## 推荐测试矩阵

固定条件：100,000 个请求、1 KiB Payload、1,000 ms Deadline。

```bash
for config in \
  "1 1" \
  "1 10" \
  "1 100" \
  "1 200" \
  "4 100" \
  "4 200" \
  "8 400" \
  "16 800"
do
  set -- $config

  ./build-simple/rpc_benchmark \
    --connections "$1" \
    --concurrency "$2" \
    --requests 100000 \
    --payload-bytes 1024 \
    --deadline-ms 1000
done
```

建议每组先预热一次，再正式运行 5 次并取中位数。正式记录期间不要同时运行
其他高负载程序。

## 初步结果

测试环境：

- 虚拟机：4 vCPU、7.7 GiB 内存。
- 宿主 CPU 型号：13th Gen Intel Core i7-13650HX。
- Linux：6.8.0-90-generic x86_64。
- 编译器：GCC 11.4.0。
- 客户端与服务端：同机，通过 `127.0.0.1` 通信。
- 服务端：4 IO 线程、4 业务线程。
- 请求：100,000 次，Payload 1 KiB，Deadline 1,000 ms。

以下数据来自首次手动单轮测试，只用于确认趋势；缺失字段未从终端截图中推测。
正式写入简历前应按相同配置重复 5 次并取中位数。

| Connections | Concurrency | QPS | P50 (ms) | P99 (ms) | Failed |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 12,309.761 | 0.073 | 0.195 | 0 |
| 1 | 10 | 60049.190 | 0.160 | 0.403 | 0 |
| 1 | 100 | 152,314.312 | 0.413 | 1.714 | 0 |
| 1 | 200 | 187,638.409 | 0.422 | 1.650 | 0 |
| 4 | 200 | 231086.350 | 0.786 | 3.047 | 0 |
| 16 | 800 | 248,703.357 | 2.766 | 6.658 | 0 |

初步结果表明，提高并发可以显著提升吞吐量，同时也会增加单个请求的延迟和
尾延迟。该结论目前只适用于本机虚拟机回环环境；还需要重复测试才能形成稳定
基线。

## 单 Reactor 初步结果

服务端配置：

```text
IO Threads:       0
Business Threads: 0
```

其他环境和请求参数与上一节相同。

| Connections | Concurrency | QPS | P50 (ms) | P99 (ms) | Failed |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 23,556.115 | 0.038 | 0.113 | 0 |
| 1 | 200 | 161,300.349 | 1.087 | 2.327 | 0 |
| 16 | 800 | 143,628.944 | 5.033 | 11.066 | 0 |

与前面的 `4 IO Threads + 4 Business Threads` 单轮结果相比：

- `1 connection / concurrency 1`：单 Reactor 的 QPS 更高、P99 更低，说明低负载
  下避免线程池投递和跨线程调度有明显收益。
- `1 connection / concurrency 200`：单 Reactor 已低于多线程配置的吞吐量，P50
  和 P99 也更高。
- `16 connections / concurrency 800`：单 Reactor 的 QPS 约为 14.36 万，多线程
  配置约为 24.87 万，说明高连接、高并发下多个 Reactor 能分担网络处理。

这组对比同时改变了 IO 线程和业务线程数量，因此只能作为架构趋势观察，不能把
差异全部归因于 Reactor 数量。要单独衡量 Reactor 扩展性，下一轮应固定
`--business-threads 0`，仅比较 `--io-threads 0/1/2/4`，每组重复 5 次并取
中位数。
