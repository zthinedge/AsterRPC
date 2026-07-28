# AsterRPC 中文说明

项目介绍、功能列表和快速开始统一维护在 [README.md](README.md)，详细设计见
[DESIGN-CN.md](DESIGN-CN.md)，各模块文档见 [docs/](docs/)。

## 分层边界

```text
业务层      CalculatorServiceImpl
业务绑定层  CalculatorStub / CalculatorServiceAdapter / Protobuf
RPC 层      RpcClient / RpcServer / PendingCalls / ServiceDispatcher
协议层      RpcMessage / RpcCodec
网络层      TcpConnection / EventLoop / Buffer
系统层      epoll / eventfd / timerfd / non-blocking sockets
```

项目没有独立的序列化模块。生成的 Stub 和服务端 Adapter 直接使用 Protobuf，
完成业务对象与 `RpcMessage::payload` 之间的转换；`RpcCodec` 只负责 RPC 帧的
编码和解码，不解析具体业务对象。

## 文档入口

- [总体设计](DESIGN-CN.md)
- [网络层](docs/NETWORK.md)
- [协议层](docs/PROTOCOL.md)
- [项目结构](docs/PROJECT-STRUCTURE-CN.md)
- [ZooKeeper](docs/ZOOKEEPER.md)
- [HTTP Gateway](docs/HTTP-GATEWAY.md)
- [基准测试](docs/BENCHMARK.md)
