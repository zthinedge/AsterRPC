# AsterRPC 协议层设计

## 1. 职责边界

协议层负责在 `RpcMessage` 和 TCP 字节流之间建立明确的消息边界：

```text
发送：RpcMessage → RpcCodec::Encode → RPC frame → TcpConnection
接收：Buffer → RpcCodec::DecodeOne → RpcMessage → RPC 调用层
```

业务对象的编解码不属于 `RpcCodec`。Calculator 的 Stub 和 Adapter 直接调用
Protobuf，把业务对象转换为 `RpcMessage::payload`，或者从 payload 恢复业务对象：

```text
Protobuf request
  → SerializeToString
  → payload
  → RpcMessage
  → RpcCodec
  → Header + Meta + Payload
```

因此项目不需要独立的 `serialization` 模块，但序列化过程仍然存在于业务绑定层。

## 2. 为什么需要自定义帧

TCP 只提供连续字节流，没有 RPC 消息边界。一次 `send` 的数据可能被多次 `recv`
读到，多次 `send` 的数据也可能被一次 `recv` 读到。因此每个 RPC 帧必须携带
`meta_len` 和 `payload_len`，解码器才能判断 Buffer 中是否已经存在一条完整消息。

一条 v3 消息由三部分组成：

```text
+--------------------+-------------------+-------------------+
| Header（固定 24B） | Meta（meta_len）  | Payload（payload_len） |
+--------------------+-------------------+-------------------+
```

## 3. Header

| 偏移 | 大小 | 字段 | 用途 |
|---:|---:|---|---|
| 0 | 4 | `magic` | 识别 AsterRPC 帧，固定为 `0x41525043`（`ARPC`） |
| 4 | 1 | `version` | 协议版本，当前为 3 |
| 5 | 1 | `message_type` | Request 或 Response |
| 6 | 1 | `codec` | 业务 payload 编码，当前只支持 Protobuf |
| 7 | 1 | `flags` | 扩展标志，当前必须为 0 |
| 8 | 8 | `request_id` | 匹配请求与响应，不能为 0 |
| 16 | 4 | `meta_len` | Meta 长度 |
| 20 | 4 | `payload_len` | Payload 长度 |

所有多字节整数使用网络字节序。解码器还会限制 Meta 最大为 64 KiB、Payload
最大为 4 MiB，避免不可信长度造成无限内存分配。

## 4. Meta 与 Payload

`RpcMeta` 保存框架控制信息：

- `service_name`、`method_name`：服务端分发路由。
- `status_code`、`error_text`：调用结果和错误信息。
- `deadline_us`：绝对截止时间。
- `trace_id`、`span_id`、`parent_span_id`：调用链上下文。

Meta 由内部的 `RpcMetaCodec` 编码，字符串采用“4 字节长度 + 内容”的形式，整数
采用固定宽度网络字节序。它不是 Protobuf 消息。

`payload` 对协议层是不透明的 `std::string` 二进制数据。具体 Protobuf 类型只在
相应服务的 Stub 和 Adapter 中出现，RPC 核心不依赖 Calculator 等业务类型。

## 5. 增量解码

`RpcCodec::DecodeOne` 每次只尝试解出一条消息：

1. 不足 24 字节时返回 `NeedMoreData`。
2. 读取并校验 Header，但暂不移动 Buffer 的读下标。
3. 完整帧尚未到达时返回 `NeedMoreData`，原数据继续保留。
4. 完整帧到达后解码 Meta、复制 Payload，并从 Buffer 移除这一帧。
5. magic、版本、类型、长度或 Meta 非法时返回 `ProtocolError`。

`RpcServer` 和 `RpcClient` 在读回调中循环调用 `DecodeOne`，直到返回
`NeedMoreData`。这样既能处理半包，也能一次处理 Buffer 中的多条粘连消息；
`ProtocolError` 表示连接上的字节流已不可信，服务端会关闭连接。

## 6. 请求与响应

请求至少携带：

```text
message_type = Request
request_id
meta.service_name
meta.method_name
meta.deadline_us
payload = Protobuf 请求对象的序列化结果
```

响应复用同一个 `request_id`，客户端据此找到对应的 `PendingCall`：

```text
message_type = Response
request_id = 原请求 request_id
meta.status_code
meta.error_text
payload = Protobuf 响应对象的序列化结果
```

同一连接可以同时存在多个在途请求，即使响应乱序到达，也不会匹配错误。

## 7. TraceContext

客户端在根调用时生成 `trace_id` 和客户端 `span_id`。服务端收到请求后创建新的
服务端 Span，并通过 `parent_span_id` 指向上游 Span。服务端继续调用下游服务时，
新的请求继承相同的 `trace_id`。

`TraceScope` 在 Handler 或回调结束时恢复原线程上下文，避免 EventLoop 线程复用时
把上一条请求的 Trace 泄漏给下一条请求。
