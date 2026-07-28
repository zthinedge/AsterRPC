# AsterRPC 目录结构

```text
include/minirpc/
  cluster/         连接池、端点、重试策略和 Round Robin
  config/          运行时配置
  gateway/         HTTP Gateway 与管理 API
  health/          端点状态与主动健康检查
  loadbalance/     P2C-EWMA 负载均衡
  log/             异步日志
  metrics/         RPC 指标
  net/             Reactor 网络层
  protocol/        RPC 二进制协议和编解码器
  registry/        ZooKeeper 注册、发现与配置中心
  rpc/             客户端、服务端、PendingCalls 和服务分发
  trace/           TraceContext

src/
  */               各公共模块的实现
  protocol/detail/ 仅供协议层内部使用的 Header、Meta 编解码细节

examples/
  client/          Calculator 客户端入口
  server/          Calculator 服务端入口
  services/        .proto、生成目标、Stub、Adapter 和业务实现
  gateway/         HTTP Gateway 与 ZooKeeper 示例
  registry/        服务注册和发现示例
  logging/         异步日志示例

tests/
  net/             Buffer、EventLoop、TcpConnection 等测试
  protocol/        RpcCodec 半包、粘包和非法包测试
  rpc/             RpcClient、RpcServer 和调用链测试
  cluster/         连接池和 Round Robin 测试
  loadbalance/     P2C-EWMA 测试
  health/          主动健康检查测试
  registry/        ZooKeeper 集成测试
  gateway/         HTTP/JSON、Protobuf 和管理 API 测试

benchmarks/
  rpc_bench.cpp    RPC 基线压测工具
```

核心原则：

1. `net` 不知道 RPC，只处理 fd、事件、连接和 Buffer。
2. `protocol` 不知道业务对象，只处理 `RpcMessage <-> bytes`。
3. `rpc` 不知道具体 Protobuf 类型，只处理 payload、request_id、注册分发和错误。
4. Stub 和 Adapter 直接使用 Protobuf 完成业务对象与 payload 的转换，不设置独立的 `serialization` 模块。
5. 示例业务只能放在 `examples/services`，不能污染框架核心。
