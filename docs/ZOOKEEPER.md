# ZooKeeper 服务注册

ZooKeeper 支持以可选模块构建，不影响没有安装 ZooKeeper C 客户端的
基础 RPC 库。

## 依赖与构建

Ubuntu 安装多线程 C 客户端：

```bash
sudo apt install libzookeeper-mt-dev
```

启用注册中心模块：

```bash
cmake -S . -B build-zk \
  -DASTERRPC_WITH_ZOOKEEPER=ON
cmake --build build-zk -j
```

业务程序需要链接 `asterrpc_zookeeper`。

## Provider 节点

Provider 使用持久父节点和临时顺序子节点：

```text
/aster-rpc/services/UserService/providers/instance-xxxxxxxxxx
/aster-rpc/services/OrderService/providers/instance-xxxxxxxxxx
```

节点数据为 Provider 的 `ip:port`。

```cpp
using namespace asterrpc;

registry::ZooKeeperClientOptions options;
options.servers="127.0.0.1:2181";

auto client=std::make_shared<registry::ZooKeeperClient>(options);
registry::ZooKeeperProvider provider(client);

client->Start();
if(!client->WaitUntilConnected(std::chrono::seconds(5))){
    throw std::runtime_error("ZooKeeper connect timeout");
}

provider.Register(
    "UserService",
    cluster::Endpoint("127.0.0.1",9000)
);
```

Consumer 首次拉取 Provider，同时注册一次性 child watch：

```cpp
registry::ZooKeeperDiscovery discovery(client);
discovery.WatchService("UserService");

registry::DiscoveryResult result=
    discovery.Resolve("UserService");

if(result.status==registry::DiscoveryStatus::NoProvider){
    // 当前没有可用实例
}
```

Watch 触发后会重新拉取全部 Provider，并在同一次操作中重新注册 Watch。
本地缓存使用 `shared_ptr<const vector<Endpoint>>` 不可变快照更新，调用线程
读取旧快照或新快照，不会观察到更新到一半的数据。普通断线期间保留最后
一次成功快照，新 Session 建立后会重新拉取。

## 配置中心与热更新

配置中心同时监听全局节点和服务节点：

```text
/aster-rpc/config/global
/aster-rpc/config/UserService
```

节点数据为 JSON 对象。服务级配置覆盖同名的全局配置，没有覆盖的字段
继续继承全局值：

```json
{
  "default_timeout_ms": 1200,
  "retry_count": 1,
  "load_balancer": "p2c_ewma",
  "health_check_interval_ms": 2000,
  "failure_threshold": 3,
  "ewma_alpha": 0.2
}
```

| 字段 | 含义 | 合法值 |
|---|---|---|
| `default_timeout_ms` | 默认调用超时 | 非负整数 |
| `retry_count` | 失败后的重试次数 | 非负整数 |
| `load_balancer` | 负载均衡算法 | `round_robin` / `p2c_ewma` |
| `health_check_interval_ms` | 主动探测周期 | 正整数 |
| `failure_threshold` | 连续失败摘除阈值 | 正整数 |
| `ewma_alpha` | 新延迟样本的权重 | `(0, 1]` |

Consumer 启动时先拉取配置，再注册一次性 data watch；Watch 触发后重新
拉取并重新注册。调用线程读取
`shared_ptr<const RpcConfig>` 不可变快照，不需要在整个请求期间持锁。
JSON 非法、字段未知或数值越界时会调用错误回调，并继续使用上一份有效
配置。

```cpp
registry::ZooKeeperConfigCenter config_center(client);
config_center.WatchService("UserService");

auto listener=config_center.Subscribe(
    "UserService",
    [](config::ConfigStore::Snapshot snapshot){
        // 将 snapshot 中的参数更新到负载均衡器和健康检查器。
    }
);
```

Registry 示例已经将热配置接入默认超时、重试、RoundRobin/P2C-EWMA
切换、健康检查周期、失败阈值和 EWMA 参数。`9002` 可人为增加 200ms
延迟：

```bash
./build-zk/registry_server 9001
./build-zk/registry_server 9002 127.0.0.1:2181 127.0.0.1 200
./build-zk/registry_client 127.0.0.1:2181 80 250
```

运行期间修改 `/aster-rpc/config/RegistryDemoService` 的
`load_balancer`，客户端无需重启即可切换算法。P2C-EWMA 会先探索每个
冷节点；获得 200ms 延迟样本后，慢节点的分数上升，后续流量会主要转向
9001。

客户端日志写入 `logs/registry_client.log`，服务端日志写入
`logs/registry_server_<port>.log`。同一次调用在两端具有相同
`trace_id`，服务端创建新的 `span_id` 并记录上游调用 span 为
`parent_span_id`，可直接用 `trace_id` 串联调用日志。

## 动态实例验收

`RoundRobin` 直接读取服务发现发布的不可变快照，使用原子序号轮询。实例
数量变化后，每次选择都会对当前快照大小取模，不会沿用旧快照的数组下标。
P2C-EWMA 的选择具有随机性，因此动态发现验收只检查实例快照是否收敛以及
剩余实例能否继续处理请求；RoundRobin 的严格轮询由单元测试验证。

一键验收需要当前用户能够访问 Docker daemon：

```bash
./scripts/test_zookeeper_discovery.sh
```

如果 ZooKeeper 已经由其他方式启动，或者当前用户无权访问 Docker daemon，
可以复用现有服务：

```bash
ASTERRPC_ZOOKEEPER_SERVERS=127.0.0.1:2181 \
  ./scripts/test_zookeeper_discovery.sh
```

脚本会：

1. 使用 `zookeeper:3.9` 启动 ZooKeeper。
2. 启动 `9001`、`9002` 两个 `RegistryDemoService` 实例。
3. 启动一个持续轮询的客户端。
4. 动态加入 `9003`，验证客户端不重启即可发现。
5. 使用 `SIGKILL` 终止 `9001`，等待 Session 过期后验证它不再被选择。

普通网络闪断由 ZooKeeper 客户端在原 Session 中自动重连，不会重复注册。
Session 过期时封装会销毁旧 `zhandle_t`、创建新 Session；Provider 在新
Session 连接成功后重新创建临时顺序节点。

连接状态回调在客户端的管理线程执行。回调可以更新本地状态，但不要在
回调内部销毁 `ZooKeeperClient`；需要关闭时应投递到调用方自己的线程。

## 集成测试

默认测试只验证参数和节点路径。连接真实 ZooKeeper 的测试需要设置：

```bash
ASTERRPC_ZOOKEEPER_TEST_SERVERS=127.0.0.1:2181 \
  ctest --test-dir build-zk -R zookeeper --output-on-failure
```
