# paseo-relay-rs

[paseo-relay](https://github.com/getpaseo/paseo-relay) 的单节点、协议兼容的 Rust
（tokio + axum）重写版本，实现了 `doc/REWRITE.md` 中的方案。

守护进程与客户端通过 `serverId` 会合；帧由 Paseo 协议进行端到端加密，因此中继
永远看不到内容。仅提供 **v2** 协议服务——`v=1`（或缺失 `v`）会被拒绝并返回
`400 Invalid protocol version`。分布式所有权（Syn/集群/重路由）和 provider
适配器被有意排除在范围之外。

## 构建与测试

```sh
cargo build --release
cargo test
```

## 运行

```sh
PASEO_RELAY_PORT=4000 ./target/release/paseo-relay
```

WebSocket 端点：`GET /ws?serverId=X&role=server|client&v=2[&connectionId=Y]`
运维端点：`GET /health`、`GET /ready`、`GET /metrics`（Prometheus 文本格式）。

## Web UI

中继二进制内嵌了构建好的 Paseo Web App（保存在 `web-ui/` 目录中），并以
SPA fallback 方式在 `/` 提供，因此分发时只需一个二进制文件。可以直接打开
offer 链接：

```text
http://<中继地址>/#offer=<base64-offer>
```

要从相邻的 `../paseo` 检出刷新内嵌应用，请运行 `scripts/vendor-web-ui.sh`
（或将导出目录作为参数传入），然后重新构建。

设置 `PASEO_RELAY_DEBUG=true` 可在 `/debug` 启用一个小的内嵌调试配对控制台：
解析 `#offer` 片段、以 `role=client` 连接中继的 `/ws` 端点，并在浏览器内完成
Paseo E2EE 握手（NaCl box）。

## 配置

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `PASEO_RELAY_HOST` | `127.0.0.1` | 监听 IP |
| `PASEO_RELAY_PORT` | `4000` | 监听端口 |
| `PASEO_RELAY_DRAIN` | `false` | 以 drain（排水）模式启动 |
| `PASEO_RELAY_MAX_CONNECTIONS` | `20000` | 最大活动 WebSocket 连接数 |
| `PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS` | `15000` | 升级前的 HTTP 空闲超时 |
| `PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS` | `5000` | 容量变更超时（超时会导致运行时重启） |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | 加权的入站字节预算 |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | 每入站字节的内存权重 |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | 每次投递的最长等待时间 |
| `PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS` | `35000` | TCP 发送超时（必须大于投递超时） |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | 每个目的地的控制队列上限 |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | 客户端帧等待数据 socket 的时长 |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | 监听器的接收缓冲区 |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | 用于分流（shedding）的 RSS 水位线（0 = 禁用） |
| `PASEO_RELAY_DEBUG` | `false` | 是否在 `/debug` 提供内嵌调试配对控制台 |

## 架构

每个有状态的组件都是一个 tokio actor（任务 + 通道 + oneshot 应答）：

- **Capacity**（`src/capacity/`）——串行化的资源账本：连接准入、加权入站预算、
  内存水位分流。一次卡住的变更会取消运行时令牌；`src/node.rs` 中的监督循环会
  将 capacity 与 listener 一起重启（类似 `rest_for_one`）。
- **Ownership/Owner**（`src/ownership/`）——每个 `serverId` 在 `DashMap` 中对应
  一个 owner 任务：v2 路由表（control/data/clients）、连接替换（`1008`）、断开级联
  （`1001`/`1012`）、带 control 提示（`1011`）的 attach 等待者、30 秒后的空闲自动注销。
- **Writer**（`src/delivery/writer.rs`）——每个目的地的写序列化器，带有写屏障握手
  （一帧只有在目的地的 sink 已将其刷新到 TCP 后才算完成）和有界控制队列
  （`1013 Slow consumer`）。
- **Socket**（`src/socket.rs`）——连接生命周期：同一时刻仅一个进行中的投递
  （`delivering` 背压）、客户端帧的握手公钥校验（`1008 Invalid handshake key`）、
  在任何退出路径上都会从 owner 分离并释放容量的 fail-closed 守卫。

关闭码与 Elixir 实现保持一致；见 REWRITE.md 第 2.9 节。
