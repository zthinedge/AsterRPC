# HTTP Gateway

HTTP Gateway 将普通 HTTP/JSON 请求转换为 AsterRPC 的 Protobuf 调用：

```text
HTTP JSON
  → Protobuf Descriptor / DynamicMessage
  → RpcChannel
  → ZooKeeper 服务发现
  → ConnectionPool
  → RPC Server
  → Protobuf Response
  → HTTP JSON
```

## 路由

网关只接受以下路由：

```text
POST /rpc/{service}/{method}
```

`service` 可以使用注册的 Protobuf 完整服务名或无歧义的短名称，例如：

```text
POST /rpc/CalculatorService/Add
POST /rpc/asterrpc.example.calculator.CalculatorService/Add
```

网关通过 `ServiceDescriptor` 查找方法，再通过
`DynamicMessageFactory` 创建请求和响应对象。因此新增 Protobuf 方法时
不需要手写 JSON DTO 或 HTTP Handler，只需要向网关注册对应
`ServiceDescriptor`。

## 快速验证

启用 ZooKeeper 模块构建：

```bash
cmake -S . -B build-zk \
  -DCMAKE_BUILD_TYPE=Release \
  -DASTERRPC_WITH_ZOOKEEPER=ON
cmake --build build-zk \
  --target gateway_calculator_server http_gateway -j
```

启动 ZooKeeper：

```bash
docker compose -f deployments/zookeeper/compose.yaml up -d --wait
```

分别启动 RPC 后端和 HTTP Gateway：

```bash
./build-zk/gateway_calculator_server 9000
./build-zk/http_gateway 8080
```

发送 HTTP 请求：

```bash
curl -i \
  -X POST \
  -H 'Content-Type: application/json' \
  -H 'X-Trace-Id: demo-trace-001' \
  -d '{"a":20,"b":22}' \
  http://127.0.0.1:8080/rpc/CalculatorService/Add
```

成功响应：

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Trace-Id: demo-trace-001

{"result":42}
```

Gateway 通过 ZooKeeper 查找
`asterrpc.example.calculator.CalculatorService` 的 Provider，根据配置中心
选择 RoundRobin 或 P2C-EWMA，并使用 `ChannelManager` 复用连接池。
客户端未提供
`X-Trace-Id` 时自动生成；提供合法值时沿用该值。Gateway 日志与 RPC
服务端日志可以通过相同的 `trace_id` 串联。

## 管理 API

Gateway 同一监听端口提供只读管理 API：

| 路由 | 内容 |
|---|---|
| `GET /admin/api/services` | 已注册的 Protobuf 服务和方法 |
| `GET /admin/api/instances` | ZooKeeper 实例、连接池及 P2C 健康状态 |
| `GET /admin/api/metrics` | Endpoint 和方法级 RPC Metrics |
| `GET /admin/api/config` | 配置中心当前生效的服务配置 |
| `GET /admin/api/traces` | 最近 256 条网关 RPC 调用 |
| `GET /admin/api/health` | Gateway、ZooKeeper、服务发现就绪状态 |

例如：

```bash
curl -s http://127.0.0.1:8080/admin/api/services
curl -s http://127.0.0.1:8080/admin/api/instances
curl -s http://127.0.0.1:8080/admin/api/metrics
curl -s http://127.0.0.1:8080/admin/api/config
curl -s http://127.0.0.1:8080/admin/api/traces
curl -i http://127.0.0.1:8080/admin/api/health
```

管理接口只读取不可变快照或原子计数，不清空 Metrics。`health` 在
ZooKeeper 未连接、服务发现未就绪、没有 Provider 或没有可选实例时返回
HTTP 503，其余接口正常返回 HTTP 200。当前版本未增加鉴权，因此只应绑定
在可信网络；生产环境应在反向代理或 Gateway 中加入认证和访问控制。

## 管理页面

启动 Gateway 后访问：

```text
http://127.0.0.1:8080/admin
```

页面源文件位于 `web/admin.html`，CMake 配置时将它嵌入 Gateway
二进制，运行时不需要额外静态文件。页面不依赖 Node.js、前端框架
或外部 CDN。
页面展示服务与方法、服务实例、健康状态、QPS、P50/P95/P99、inflight、
超时、重试数和最近 Trace。默认每 2 秒刷新，也可以切换为 5 秒、10 秒或
暂停，并支持手动刷新。

QPS 由浏览器根据相邻两次 Metrics 累计请求数的差值计算；延迟分位数在
Endpoint 表中分别展示，顶部摘要取所有 Endpoint 的最大值，避免低估慢
节点。页面和 API 使用同源请求，因此不需要额外配置 CORS。

## 错误映射

| RPC/网关错误 | HTTP 状态 |
|---|---:|
| HTTP 格式或 Protobuf JSON 非法 | 400 |
| 服务、方法或路由不存在 | 404 |
| 非 POST 请求 | 405 |
| 非 `application/json` | 415 |
| RPC 内部或调用错误 | 500 |
| RPC 响应无法解码 | 502 |
| 没有 Provider、连接失败 | 503 |
| RPC Timeout/Deadline | 504 |

错误响应同样是 JSON，并携带 `error`、`message` 和 `trace_id`。

## 当前 HTTP 连接策略

第一版每个 HTTP 连接处理一个请求，响应发送完毕后关闭连接。这样可以在
当前单 Reactor 网络层上保证异步 RPC 响应不会破坏 HTTP pipeline 的响应
顺序。后续支持 keep-alive 时，需要为每条连接增加请求序号和有序响应队列。
