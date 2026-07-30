# rpc_bench

`rpc_bench` 通过真实的 AsterRPC 客户端连接调用 `BenchService.Echo`。它使用多个
客户端 EventLoop 和异步流水线维持固定并发，避免同步 Future 和单 EventLoop 限制
压测能力。

## 快速运行

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j"$(nproc)"
```

终端一：

```bash
./build-release/calculator_server 9000 \
  --io-threads 4 \
  --business-threads 0 \
  --io-balance least-connections
```

终端二：

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

完整的基线对比方法见 [性能测试文档](../docs/BENCHMARK.md)。
