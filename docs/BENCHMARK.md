# AsterRPC 性能测试

## 1. Release 构建

性能测试必须使用 Release 构建，Debug 结果不具有对比意义。

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j"$(nproc)"
ctest --test-dir build-release --output-on-failure
```

## 2. 启动服务端

以下模式一次只启动一个，端口均为 9000。

单 Reactor 基线：

```bash
./build-release/calculator_server 9000 \
  --io-threads 0 \
  --business-threads 0
```

主从 Reactor，业务仍在 IO 线程执行：

```bash
./build-release/calculator_server 9000 \
  --io-threads 4 \
  --business-threads 0 \
  --io-balance least-connections
```

主从 Reactor 加业务线程池：

```bash
./build-release/calculator_server 9000 \
  --io-threads 4 \
  --business-threads 4 \
  --business-queue 65536 \
  --io-balance least-connections
```

线程数应根据机器 CPU 核数和业务类型调整，不应固定照抄。纯 Echo 业务非常轻，
业务线程池增加的入队、唤醒和跨线程发送成本可能使 QPS 下降；它的主要价值是防止
真实的耗时业务阻塞 IO 线程。

## 3. 运行 rpc_bench

在另一个终端执行：

```bash
./build-release/rpc_bench \
  --host 127.0.0.1 \
  --port 9000 \
  --io-threads 4 \
  --connections 16 \
  --concurrency 200 \
  --requests 500000 \
  --payload 1024 \
  --timeout-ms 5000
```

参数含义：

- `--io-threads`：压测客户端 EventLoop 数量；
- `--connections`：所有客户端 EventLoop 的 TCP 连接总数；
- `--concurrency`：允许同时在途的 RPC 请求数量；
- `--requests`：总请求数；
- `--payload`：每次 Echo 请求携带的字节数；
- `--timeout-ms`：单次调用超时，设置为 0 可关闭 Deadline 定时器；
- `--host`、`--port`：目标服务端地址。

输出包含成功数、失败数、QPS，以及平均、最小、最大、P50、P95、P99 延迟。

`rpc_bench` 使用异步流水线维持固定并发，不为每个请求创建等待 Future 的工作线程。
每个客户端 EventLoop 管理一个独立连接池，从而避免压测器自己的单 EventLoop 先成为
瓶颈。`io-threads` 不得超过 `concurrency`，`connections` 不得少于
`io-threads`。

## 4. 对比方法

分别启动三种服务端模式，并对每种模式预热一次、正式测试至少三次：

```bash
for i in 1 2 3; do
  ./build-release/rpc_bench \
    --host 127.0.0.1 --port 9000 \
    --io-threads 4 --connections 16 \
    --concurrency 200 --requests 500000 \
    --payload 1024 --timeout-ms 5000
done
```

对比时必须保持机器、Release 编译参数、请求数、并发数和 payload 一致。虚拟机中的
调度抖动较大，应记录多次结果而不是只挑最高值。

同机压测时客户端与服务端竞争相同的 CPU 核，因此结果不代表独立压测机下的绝对
上限。若客户端 IO 线程已经占满 CPU，可使用另一台机器发压，或者并行启动多个
`rpc_bench` 进程并汇总吞吐量。

## 5. 当前样例结果

4 vCPU 虚拟机、本机回环地址、1 KiB payload、并发 200、16 条连接的样例结果：

| 服务端模式 | QPS 范围 | 失败数 |
|---|---:|---:|
| 单 Reactor | 245k–251k | 0 |
| 4 个 sub Reactor，无业务池 | 293k–316k | 0 |
| 4 个 sub Reactor + 4 个业务线程 | 153k–163k | 0 |

Echo Handler 只返回原字符串。业务线程池会额外经历任务入队、线程唤醒和响应投递，
所以在这种极轻业务下反而降低 QPS。业务线程池的收益需要使用 CPU 密集或阻塞业务
单独验证。

### 5.1 主从 Reactor + 业务线程池单次结果

服务端使用 4 个 sub Reactor、4 个业务线程和
`least-connections` 连接分配策略。压测客户端使用 4 个 EventLoop、16 条连接，
维持 200 个在途请求。一次 50 万请求的正式测量结果如下：

```text
client io threads: 4
connections: 16
timeout(ms): 5000
requests: 500000
payload bytes: 1024
succeeded: 500000
failed: 0
elapsed(s): 3.07
QPS: 162894.36
latency(us) avg/min/max: 1221/52/45563
latency(us) P50/P95/P99: 1023/2651/4472
```

该结果是在 4 vCPU 虚拟机上通过本机回环地址测试得到的，只作为当前实现的 Echo
基线。客户端、服务端和内核网络协议栈会竞争同一组 CPU，不能将其视为独立压测机
环境下的性能上限。

## 6. 如何解释结果

- 单 Reactor vs 主从 Reactor：观察连接 IO 能否利用多个 CPU 核。
- 主从 Reactor vs 主从 Reactor + 业务池：观察业务隔离的成本与收益。
- Echo QPS 没提升不等于业务池无效；应再使用包含计算或阻塞的 Handler 测试。
- QPS 必须和失败数、P99 延迟一起看，不能只比较吞吐量。
