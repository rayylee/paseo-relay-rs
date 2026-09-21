# paseo-relay-rs 重构方案

> **状态：已实现**（2026-09-21）。48 个测试（16 单元 + 32 集成，真实 TCP/WebSocket）
> 全部通过，clippy 零告警。

## 1. 背景与目标

### 为什么重构

当前 `[paseo-relay](https://github.com/getpaseo/paseo-relay)` 使用 Elixir/OTP 实现，在生产环境运行约 23,000 个 WebSocket 连接。
重构目标：

- **团队统一技术栈**：Rust 已是项目其他组件的主要语言
- **降低每连接内存占用**：BEAM 每进程起步约 2 KB，tokio task 可低至 200–500 字节
- **更小的部署产物**：无需携带 BEAM VM
- **FFI 友好**：便于未来与 native 组件集成

### 范围界定

| 保留 | 去除 |
|---|---|
| 全部 v2 协议语义 | v1 协议（所有现存客户端已使用 v2） |
| 单节点完整功能 | 分布式 ownership（Syn/集群） |
| 背压与容量控制 | DNS 集群发现 |
| 内存水位 shed | Reroute / 跨节点转发 |
| `/health` `/ready` `/metrics` | Fly.io 特定部署适配器 |
| 握手公钥校验 | Docker 部署配置 |

**v1 处理**：收到 `v=1` 或缺少 `v` 参数的请求，直接返回 `400 Invalid protocol version`。

---

## 2. 协议行为要求（v2）

这是重构的功能基线，所有行为必须与原实现一致。

### 2.1 连接参数

WebSocket 升级请求路径：`GET /ws?serverId=X&role=server|client&v=2[&connectionId=Y]`

| 参数 | 规则 |
|---|---|
| `serverId` | 必填，1–256 字节，超出返回 `400` |
| `role` | 必填，`server` 或 `client`，否则 `400` |
| `v` | 必填，仅接受 `2`，否则 `400` |
| `connectionId` | server 角色可选（空字符串 = control socket）；client 角色若缺省则自动生成 `conn_<16位hex>` |

### 2.2 v2 连接拓扑

每个 `serverId` 对应一个 **Owner**，管理以下连接槽：

```
serverId "X" 的 Owner
├── control socket    (role=server, connectionId="")
│     接收控制通知：sync / connected / disconnected / ping→pong
├── data socket [A]  (role=server, connectionId="conn_aaa")
│     ← 向 client [A] 转发帧
├── data socket [B]  (role=server, connectionId="conn_bbb")
│     ← 向 client [B] 转发帧
├── client [A]       (role=client, connectionId="conn_aaa")
│     → 帧转发给 data socket [A]
└── client [B]       (role=client, connectionId="conn_bbb")
      → 帧转发给 data socket [B]
```

### 2.3 帧路由规则

| 来源 | 目标 |
|---|---|
| client（connectionId=X）→ | data socket（connectionId=X） |
| data socket（connectionId=X）→ | 该 connectionId 下所有 client |
| control socket → | （只接收，不转发 payload；发出控制通知） |

### 2.4 控制通知（Owner → control socket）

Owner 向 control socket 发送的 JSON 消息（text 帧）：

```jsonc
{ "type": "sync",         "connectionIds": ["conn_aaa", "conn_bbb"] }  // control 首次连接时
{ "type": "connected",    "connectionId": "conn_aaa" }                 // client 连入
{ "type": "disconnected", "connectionId": "conn_aaa" }                 // 最后一个 client 断开
```

control socket 入站只处理 ping：
```jsonc
{ "type": "ping" }  →  { "type": "pong", "ts": <unix_ms> }
```

### 2.5 连接替换语义

新连接 attach 时，若同类型旧连接已存在：

| 场景 | 行为 |
|---|---|
| 新 control socket 连入 | 旧 control socket 收到 `1008 Replaced by new connection` |
| 新 data socket（同 connectionId）连入 | 旧 data socket 收到 `1008 Replaced by new connection` |
| client 同一 connectionId 多个连接 | 并存（MapSet），同时收帧 |

### 2.6 连接断开级联

| 事件 | 级联行为 |
|---|---|
| 最后一个 client（connectionId=X）断开 | data socket（X）收到 `1001 Client disconnected`；control 收到 `disconnected` 通知 |
| data socket（connectionId=X）断开 | 该 connectionId 下所有 client 收到 `1012 Server disconnected` |
| control socket 断开 | 无级联（daemon 会重连） |

### 2.7 client 等待 data socket（attach_timeout）

client 连入时，若对应 connectionId 的 data socket 尚未 attach：

1. 收到 client 帧时，delivery task 等待 data socket，超时 = `min(delivery_deadline, data_attach_timeout_ms)`
2. 等待期间，Owner 向 control socket 发 `sync` nudge（10 秒后），再等 5 秒后若 data socket 仍未到，关闭 control socket（`1011 Control unresponsive`）
3. attach timeout → client 收到 `1013 Data route unavailable`

### 2.8 握手公钥校验

仅对 **client** 角色的 text/binary 帧做检查。判断条件：

- JSON 可解析
- `type` 字段为 `"hello"` 或 `"e2ee_hello"`

若命中，校验 `key` 字段（X25519 公钥）：

1. 必须是 canonical padded Base64（round-trip 验证）
2. 解码后恰好 32 字节
3. 必须是 Curve25519 域内合法坐标（little-endian 256-bit 整数 < 2²⁵⁵ − 19）
4. 不在 7 个已知弱/低阶公钥黑名单中

校验失败 → `1008 Invalid handshake key`，**不转发**。

### 2.9 关闭码汇总

| 代码 | 含义 | 触发场景 |
|---|---|---|
| `1001` | Client disconnected | data socket 因 client 全部断开而关闭 |
| `1008` | Replaced by new connection | 同类型新连接取代旧连接 |
| `1008` | Invalid handshake key | 握手公钥校验失败 |
| `1009` | Frame too large | 帧超过上限（控制 socket 64 KiB，其余 32 MiB；应用层校验 + axum `max_message_size` 兜底） |
| `1011` | Control unresponsive | nudge 后 5 秒 control 仍未响应 |
| `1012` | Session expired | Owner 已不存在或 reservation 过期 |
| `1012` | Server disconnected | data socket 断开，级联关闭其 clients |
| `1013` | Relay ingress capacity | 入站字节预算耗尽 |
| `1013` | Data route unavailable | client 帧等待 data socket 超时 |
| `1013` | Delivery unavailable | Writer deadline 超时或目标已关闭 |
| `1013` | Relay memory pressure | 内存水位触发 shed |
| `1013` | Relay capacity unavailable | Capacity actor 不可用 |
| `1013` | Slow consumer | 目标写入队列超出 control_queue_bytes |

---

## 3. 架构设计

### 3.1 整体结构

```
main()
└── node::start()               # 绑定监听 socket，进入 supervisor loop
    └── supervisor_loop()       # 替代 OTP rest_for_one（node.rs）
        ├── CapacityActor       # 容量账本，fail-closed 关键进程
        └── axum Listener       # HTTP + WebSocket，绑定 CancellationToken
            ├── GET /health
            ├── GET /ready
            ├── GET /metrics
            └── GET /ws → ws_handler()
                    └── SocketTask  # 每连接一个 tokio task
                        └── WriterTask  # 每连接一个专属写入序列化器
```

**单例共享状态**（`Arc<AppState>`，注入所有 handler）：

```rust
pub struct AppState {
    pub config:    Arc<Config>,
    pub ownership: Ownership,        // 内部为 Arc<DashMap<ServerId, OwnerHandle>>
    pub capacity:  CapacityHandle,
    pub drain:     Arc<Drain>,
    pub metrics:   Arc<Metrics>,
}
```

**实现说明**：supervisor 重启会创建新一代 `AppState`（新的 Capacity handle 与
Ownership 表），通过 `tokio::sync::watch` 通道发布给外部持有者（`Node::state`）；
`Drain` 与 `Metrics` 跨代共享。监听 socket 在重启时按原地址重新 bind（失败则
200ms 间隔重试），进程内计数器不清零。

### 3.2 模块职责

```
src/
├── main.rs               # 入口：日志、配置加载、ctrl-c 排空
├── lib.rs                # 模块声明 + next_id()（全局唯一 ID，make_ref() 等价）
├── node.rs               # supervisor loop + axum 路由 + ws_handler()
├── config.rs             # 环境变量解析与校验
├── protocol.rs           # 协议常量（最大帧大小等）
├── connection.rs         # Connection 结构体，query string 解析
├── handshake.rs          # X25519 握手公钥校验
├── drain.rs              # 优雅排空状态（AtomicBool）
├── metrics.rs            # 指标计数器与 Prometheus 文本渲染
├── operations.rs         # /health /ready /metrics handler
├── ownership/
│   ├── mod.rs            # Ownership：DashMap + route() 逻辑
│   └── owner.rs          # OwnerTask + OwnerHandle（tokio actor）
├── capacity/
│   └── mod.rs            # CapacityTask + CapacityHandle（tokio actor）
├── delivery/
│   ├── mod.rs            # deliver()：并发向多个 Writer 投递
│   └── writer.rs         # WriterTask + WriterHandle（tokio actor）
└── socket.rs             # WebSocket 连接全生命周期 + SocketGuard
```

### 3.3 Actor 通信模型

所有有状态组件都是 tokio actor（task + mpsc channel）。调用方通过 `Handle` 结构体持有发送端，回复通过 `oneshot::channel` 返回。

```
SocketTask ──AdmitConnection──▶ CapacityTask
           ──Route/Reserve───▶ OwnerTask
           ──Attach──────────▶ OwnerTask
           ──AdmitMessage────▶ CapacityTask
           ──Destinations────▶ OwnerTask
           ──Reserve/Write───▶ WriterTask
           ◀─SocketMsg────────  WriterTask（帧、关闭、压力信号）
```

---

## 4. 关键模块详设

### 4.1 Supervisor Loop（替代 OTP rest_for_one）

实现于 `node.rs`。每一"代"（generation）= 一个 Capacity actor + 一个 axum
listener，共享一个从根 token 派生的 child `CancellationToken`：

```rust
// 简化后的实际结构（node.rs::supervisor_loop）
loop {
    // 本代：router 持有本代的 Arc<AppState>
    let listener_done = tokio::spawn(axum::serve(listener, router)
        .with_graceful_shutdown(generation_cancel.cancelled()));

    tokio::select! {
        _ = &mut capacity_done => generation_cancel.cancel(),  // rest_for_one 级联
        _ = generation_cancel.cancelled() => {}
    }
    let _ = listener_done.await;

    if root_cancel.is_cancelled() { return; }   // 进程级关停

    tracing::error!("supervisor: restarting capacity + listener");
    tokio::time::sleep(Duration::from_millis(100)).await;
    listener = rebind(local_addr).await;         // 原地址重新 bind，失败重试
    (state, capacity_done) = build_generation(); // 新一代 Capacity + AppState
    state_tx.send(state.clone());                // watch 通道发布新 state
}
```

**Capacity timeout kill 语义**：CapacityHandle 的 mutation 方法内，若
`tokio::time::timeout` 超时，调用 `cancel.cancel()` 触发整体重启，等价于
Elixir 的 `Process.exit(capacity, :kill)` + `rest_for_one` 级联。

**实现说明**：根 token 与代 token 分层（`root_cancel.child_token()`），使
ctrl-c 关停与"代内崩溃重启"互不干扰；监听端口跨代保持。

### 4.2 Ownership（本地 DashMap 替代 Syn）

```rust
pub struct Ownership {
    owners: Arc<DashMap<String, OwnerHandle>>,
}

impl Ownership {
    pub async fn route(&self, server_id: &str, draining: bool) -> RouteResult {
        // 查到的 owner 可能刚好空闲退出（stale handle），
        // 因此最多重试一轮：查表 → 无则（非 drain 时）entry API 原子创建 → reserve。
        for _attempt in 0..2 {
            let (owner, created) = /* 查表或 or_insert_with 创建 */;
            match owner.reserve().await {
                Ok(reservation) => return RouteResult::Local { owner, reservation },
                Err(_) if !created => continue,   // stale：重试并新建
                Err(_) => return RouteResult::Unavailable,
            }
        }
        RouteResult::Unavailable
    }
}
```

Socket 退出时通过 `SocketGuard::drop` 机制自动触发 `Detach` 消息；Owner 空闲
30 秒后自注销，退出时用 `remove_if` 比对 channel 同一性后再从 DashMap 删除
自身（防止误删已被新 owner 占用的槽位）。

**实现说明**：drain 检查只拦截"新建 owner"，已有 owner 的 reserve 不受
drain 影响（与原实现一致——drain 只拒新会话，不断已有会话）。

### 4.3 Owner Actor 状态

```rust
struct OwnerState {
    server_id: String,
    owners:    Arc<DashMap<String, OwnerHandle>>,
    sender:    mpsc::UnboundedSender<Msg>,   // 自引用，用于 send_after 定时消息

    // 预留槽：token → 过期时刻
    reservations: HashMap<u64, Instant>,

    // 已 attach 的 socket
    sockets: HashMap<u64, SocketEntry>,      // {connection, writer}

    // v2 路由表
    control: Option<u64>,                    // connectionId=""
    data:    HashMap<String, u64>,           // data sockets
    clients: HashMap<String, HashSet<u64>>,  // client sockets

    // 等待 data socket 的 client 帧投递请求（reply 为 oneshot Sender）
    waiting: HashMap<u64, Waiter>,

    // 空闲起点；send_after(IdleCheck) 在 30s 后核对
    idle_since: Option<Instant>,
}
```

**进程存活追踪**：不使用 `Process.monitor`，改为每个 SocketTask 在 `Drop`
（`SocketGuard`）时主动发送 `OwnerMsg::Detach`，语义等价。

**定时消息**：`Process.send_after` 等价物为 spawn 一个 `sleep + send` 的小
task（reservation 过期、nudge 10s、reset 5s、idle 30s 均走此机制）；消息到达
时先核对状态仍然成立再执行，天然容忍迟到的定时器。

### 4.4 Capacity Actor

单一 GenServer 风格的资源账本，所有状态变更串行化：

```rust
struct CapacityState {
    max_connections: usize,
    ingress_limit:   usize,         // 默认 512 MiB
    ingress_weight:  usize,         // 默认 4
    watermark:       usize,         // 0 = 禁用

    connections: HashMap<u64, ConnStatus>,   // Reservation{expires_at} | Active{socket_id}
    sockets:     HashMap<u64, SocketCapState>,
    messages:    HashMap<u64, MsgEntry>,

    // shed 优先级队列，key = (Instant, sequence)
    // oldest-blocked-first，newest-active-last（LIFO）
    blocked: BTreeMap<(Instant, u64), u64>,
    active:  BTreeMap<(Instant, u64), u64>,

    // 计数器（active_reservations + active_websockets = 占用的连接额度）
    active_reservations: usize,
    active_websockets:   usize,
    reserved_bytes:      usize,
    inflight_bytes:      usize,
    blocked_sources:     usize,

    pressure: Option<PressureState>,   // {memory, victims, batch}
}
```

**内存 shed 策略**（每秒检查 + 压力期 100ms 复查）：

1. 读取当前 RSS（默认 `/proc/self/status` VmRSS；构造时可注入
   `MemoryProbe` 闭包，集成测试用它模拟内存压力）
2. 若 > watermark，计算 batch_size（首轮基于超出量 / 最大帧大小，上限 64；
   后续轮次按上一轮实际释放效果自适应，上限 1024）
3. 优先 shed `blocked` 最老的 socket（占用内存最多）
4. 其次 shed `active` 最新的 socket（保护老会话）
5. 向 socket task 发送 `SocketMsg::MemoryPressure`，socket 自行关闭并发 `1013`
6. 内存回落到 recovery 阈值（watermark − 最大帧大小）以下时解除压力态

**mutation 超时处理**：所有状态变更调用都有 `capacity_mutation_timeout_ms`
超时，超时时触发 `cancel.cancel()`，supervisor 重启整体。读取（status）使用
独立的 1 秒超时，不触发重启。

**实现说明**：原实现的 namespace 化连接预算（`{namespace, limit}`）在单节点
单 listener 下退化为一个全局 `max_connections`，未保留 namespace 维度；
连接 reservation 的 5 秒过期由每秒 tick 统一清理，不再逐条挂定时器。

### 4.5 Writer Actor

每个目标 WebSocket 连接的专属写入序列化器，确保同一时刻只有一个 payload 在写：

```
状态机：idle → reserved → writing → idle
                                ↑________↓（barrier ack 后继续）
```

**write barrier 机制**（对应 Elixir `{:relay_write_barrier, ...}`）：

1. Writer 向目标 socket 发 `SocketMsg::Frame`
2. 同时发 `SocketMsg::Barrier { reference, writer }`（携带 WriterHandle，
   目标 socket 无需另行持有 writer 引用）
3. SocketTask 的写出循环收到 Barrier 后执行 `sink.flush()`，完成即回
   `writer.acknowledge(reference)`
4. Writer 收到 `Written` 后解锁 active slot，处理队列下一项

控制消息（sync/connected/ping→pong）与 payload 共用同一 active slot 串行化，
但排队字节独立计入 `control_queue_bytes` 上限，超出则关闭目标
（`1013 Slow consumer`）。

**source 存活追踪**（方案初稿未覆盖、按原实现补齐的关键语义）：
`reserve()` 会返回一个 oneshot「存活信号」，source socket 退出即触发。
若 source 在 **帧已发往目标** 后死亡，帧状态不明，fail closed——关闭目标
（`1013 Delivery unavailable`）；若在发帧前死亡，仅释放预留槽继续下一项。
对应 Elixir Writer 里对 `source_ref` 的两条 `:DOWN` 分支。

### 4.6 Socket 生命周期

```
ws_handler()
  │
  ├─ 解析 Connection（400 on error）
  ├─ capacity.admit_connection() → ConnToken  （503 on reject）
  ├─ ownership.route() → RouteResult::Local { owner, reservation }
  │                      （Unavailable → 释放 ConnToken，503）
  │
  └─ WebSocket 升级 → handle_socket()
       ├─ capacity.attach_connection()       （1012 on fail）
       ├─ WriterTask::spawn()
       ├─ SocketGuard                        （Drop 时自动 Detach + release capacity）
       ├─ owner.attach(reservation, socket_id, connection, writer)  （1012 on fail）
       │
       └─ 单一事件循环（socket.rs::run，一个 select! 统一三路事件）：
           loop {
             select! {
               signal = signal_rx.recv()
                 → Frame：写入 sink
                 → Barrier：sink.flush() 后 writer.acknowledge()
                 → Close/MemoryPressure：发关闭帧退出
               outcome = delivery_done, if delivering
                 → finish_message；Ok 则处理 pending 队列，Err 则按关闭码退出
               frame = stream.next(), if !delivering
                 → admit_message → 握手校验 → start_delivery（暂停读取）
             }
           }
```

**背压控制**：`delivering`（即 `delivery: Option<...>` 是否为 Some）控制是否
继续从 stream 读帧，等价于 Elixir 的 `{active, false}` / `{active, true}`。

**实现说明**：与方案初稿的差异——(1) 未拆出独立 sink task，写出与读入合并在
同一个事件循环里（Frame/Barrier 属 signal 分支），省一个 task 且天然避免
sink 竞争；(2) `admit_connection` 提前到 route 之前，保证容量拒绝先于 owner
创建；(3) 帧大小上限在应用层校验（控制 socket 64 KiB，其余 32 MiB），
超限回 `1009`，同时也配置了 axum 的 `max_message_size` 兜底。

### 4.7 帧投递流程

```
handle_frame(message)
  ├─ capacity.admit_message(bytes)          → MsgToken（预留 bytes × weight 配额）
  ├─ handshake::check(payload)              → 仅 client 角色
  │   reject → capacity.finish_message(); close 1008
  ├─ capacity.start_delivery(token)         → 登记 inflight、标记 blocked
  └─ spawn 投递 task（socket 主循环停读，等 oneshot 结果）：
       ├─ owner.destinations(source, deadline, attach_timeout)
       │    → Writers(Vec<WriterHandle>) 或 Control（不转发）
       └─ delivery::deliver(writers, message, deadline)
            ├─ 并发 task：writer.reserve(deadline) → (token, 存活信号)
            │             writer.write(token, message, deadline)
            └─ 至少一个成功 → Ok()（无目标时空转发也算成功）
  └─ 结果返回主循环 → capacity.finish_message(token)，恢复读取
```

---

## 5. 配置项

与原实现对应，去掉集群相关项。`PASEO_RELAY_MAX_CONNECTIONS` 为新增项，替代
原实现的 `PASEO_RELAY_ACCEPTORS × PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR`
（100 × 200 = 20000，默认值保持等值）；原实现的
`PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS`（BEAM per-process heap 上限）无
tokio 等价物，未保留。所有取值范围与校验规则（含
`INGRESS_BUDGET ≥ 最大帧 × weight`、`DELIVERY_TIMEOUT < TRANSPORT_SEND_TIMEOUT`）
与原实现一致：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `PASEO_RELAY_HOST` | `127.0.0.1` | 监听 IP |
| `PASEO_RELAY_PORT` | `4000` | 监听端口 |
| `PASEO_RELAY_DRAIN` | `false` | 启动即进入排空模式 |
| `PASEO_RELAY_MAX_CONNECTIONS` | `20000` | 节点最大活跃 WebSocket 数 |
| `PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS` | `15000` | 升级前 HTTP 空闲超时 |
| `PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS` | `5000` | 状态变更操作超时（超时触发重启） |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | 节点入站字节总预算（512 MiB） |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | 每字节内存权重系数 |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | 单次投递最大等待时间 |
| `PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS` | `35000` | TCP 发送超时（须 > delivery timeout） |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | 每目标控制消息队列上限（1 MiB） |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | client 帧等待 data socket 的超时 |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | 每 socket TCP 接收缓冲区 |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | 内存水位（0 = 禁用 shed） |

---

## 6. HTTP 端点

### `GET /health`

始终返回 `200`，用于存活探针：

```json
{"status": "ok"}
```

### `GET /ready`

返回 `200` 的条件（全部满足）：

- 不在 drain 模式
- Capacity actor 可达且未处于内存压力
- 连接数未满

任一条件不满足返回 `503`：

```json
{"status": "unready"}
```

### `GET /metrics`

Prometheus 文本格式（`text/plain; version=0.0.4`），暴露以下指标：

| 指标 | 类型 | 说明 |
|---|---|---|
| `paseo_relay_ready` | gauge | 节点是否就绪 |
| `paseo_relay_draining` | gauge | 是否处于 drain 模式 |
| `paseo_relay_active_websockets` | gauge | 当前活跃 WebSocket 数 |
| `paseo_relay_active_sessions` | gauge | 当前 Owner 数（serverId 数） |
| `paseo_relay_reroute_responses_total` | counter | （单节点恒为 0，保留字段） |
| `paseo_relay_connection_rejections_total` | counter | 因容量/压力拒绝的升级数 |
| `paseo_relay_frames_forwarded_total` | counter | 已转发帧数 |
| `paseo_relay_bytes_forwarded_total` | counter | 已转发字节数 |
| `paseo_relay_ingress_reserved_bytes` | gauge | 当前入站字节预留量 |
| `paseo_relay_inflight_delivery_bytes` | gauge | 投递中的字节数 |
| `paseo_relay_backpressured_sources` | gauge | 当前因背压暂停的 source 数 |
| `paseo_relay_slow_consumer_disconnects_total` | counter | 因超时关闭的慢消费者数 |
| `paseo_relay_delivery_timeouts_total` | counter | 投递超时次数 |
| `paseo_relay_memory_pressure_disconnects_total` | counter | 内存压力 shed 次数 |
| `paseo_relay_max_frame_bytes` | gauge | 历史最大帧 payload |
| `paseo_relay_delivery_wait_seconds` | histogram | 投递等待时间分布 |
| `paseo_relay_frame_size_bytes` | histogram | 帧大小分布 |
| `paseo_relay_handshake_accepted_total` | counter | 握手公钥校验通过数（按 type 分，`routing_version` 恒为 `v2`） |
| `paseo_relay_handshake_rejected_total` | counter | 握手公钥校验拒绝数（按 type 分，同上） |
| `paseo_relay_process_memory_bytes` | gauge | 进程 RSS（/proc/self/status VmRSS） |

**实现说明**：原 `paseo_relay_beam_*_memory_bytes` 系列是 BEAM 专属指标，
Rust 进程只有 RSS 一个有意义的内存读数，故合并为
`paseo_relay_process_memory_bytes`——监控面板迁移时需要同步改表达式。
Capacity 相关 gauge（active_websockets / ingress_reserved_bytes /
inflight_delivery_bytes / backpressured_sources）在 Capacity actor 不可达时
从输出中省略，与原实现行为一致。

---

## 7. 依赖选型

```toml
[dependencies]
tokio          = { version = "1",    features = ["full"] }
axum           = { version = "0.8",  features = ["ws"] }
tokio-util     = "0.7"         # CancellationToken
bytes          = "1"
serde          = { version = "1",    features = ["derive"] }
serde_json     = "1"
dashmap        = "6"           # 并发 HashMap，serverId → OwnerHandle
base64         = "0.22"        # 握手公钥 canonical Base64 round-trip
rand           = "0.8"         # connectionId 生成
futures-util   = "0.3"         # WebSocket stream/sink
form_urlencoded = "1"          # query string 解析
socket2        = "0.5"         # 监听 socket 接收缓冲区设置
tracing        = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tokio-tungstenite = "0.24"     # 集成测试 WebSocket 客户端
```

**选型说明**：

- `axum`：原生 WebSocket 升级支持，与 tokio 深度集成，handler 即 async fn
- `dashmap`：无锁读多写少场景，替代 Syn 分布式注册
- ~~`x25519-dalek`~~ **未采用**：握手校验只需 canonical Base64 round-trip、
  32 字节长度、小端 256-bit 整数 < 2²⁵⁵−19、7 个弱公钥黑名单比对——全部是
  纯字节运算（`handshake.rs` 约 100 行含 const 黑名单），引入曲线库反而
  增大依赖面
- ~~`prometheus-client`~~ **未采用**：原实现即手写 Prometheus 文本，Rust 版
  照做（`AtomicU64` 计数器 + 文本渲染），输出格式与原版逐行对齐，便于
  监控无缝迁移
- ~~`tower`~~ **未采用**：axum 自带能力已够，无需显式依赖

---

## 8. 与原实现的对应关系

| Elixir 机制 | Rust 实现 |
|---|---|
| `:cowboy_websocket` | `axum::extract::ws::WebSocketUpgrade` |
| `GenServer` actor | `tokio task` + `mpsc::channel` + `oneshot` 回复 |
| `make_ref()` | `next_id()`（进程级 `AtomicU64`，lib.rs） |
| `Process.monitor` socket + `:DOWN` | `SocketGuard::drop` 发 `Detach` 消息 |
| Writer monitor source | `reserve()` 返回的 oneshot 存活信号，drop 即触发 |
| Writer monitor destination | `WriterDestinationGuard`（socket 退出时 drop） |
| `Process.send_after` | spawn `sleep + send` 小 task，收到后核对状态仍成立 |
| `rest_for_one` 监督树 | 根/代两层 `CancellationToken` + supervisor loop（node.rs） |
| Capacity timeout kill | mutation 超时 → `cancel.cancel()` 触发整代重启 |
| `{active, false/true}` 流控 | `delivering` 状态 + `select!` guard |
| `{:relay_write_barrier, ...}` | `SocketMsg::Barrier` → `sink.flush()` → `acknowledge()` |
| Syn 分布式注册 | `Arc<DashMap>`（本地，单节点；`remove_if` 防误删） |
| `Agent`（Drain） | `AtomicBool` |
| `:erlang.memory(:total)` | `/proc/self/status` VmRSS（可注入 `MemoryProbe` 供测试） |
| `:gb_trees` shed 队列 | `BTreeMap<(Instant, u64), u64>` |
| `active_n: 1` | tokio select 中的 `delivering` guard |
| `:counters` / `:atomics` 指标 | `AtomicU64`（含 `fetch_max` 实现 max_frame_bytes） |

---

## 9. 测试策略（已落地：48 个测试全绿）

所有测试使用真实 TCP + 真实 WebSocket，**不 mock** 任何被测组件。
单元测试 16 个（config / connection / handshake），集成测试 32 个，分布：

### 9.1 核心行为测试（`tests/routing_test.rs`，15 个）

- **路由**：v2 control 连入收到 sync（含已有 client 列表）；client 连入
  control 收到 connected；最后一个 client 断开收到 disconnected；
  control ping→pong（含 ts）
- **参数校验**：v1 / 缺 v / 缺 role / 缺 serverId / 超长 serverId → `400`
- **帧转发**：client → data socket 有序转发 text/binary；data socket →
  多 client 广播；不同 connectionId 双向隔离
- **连接替换**：新 control/data socket 取代旧的，旧的收到 `1008`
- **级联断开**：data socket 断开，clients 收到 `1012 Server disconnected`；
  client 全断，data socket 收到 `1001 Client disconnected`（部分断开不触发）；
  control 断开无级联
- **client 等待 data socket**：client 先发帧，data socket 后 attach 仍收到；
  owner 空闲复用/重连

### 9.2 背压与容量测试（`tests/capacity_test.rs`，7 个）

- **入站字节预算**：预算耗尽后的消息触发 `1013 Relay ingress capacity`
- **投递背压**：50 帧快速连发保持有序；等待投递不阻塞反向流量
- **attach 超时**：无 data socket 的 client 帧超时收到 `1013 Data route unavailable`
- **连接数上限**：超过 max_connections 收到 `503`，断开后额度回收
- **内存 shed**：注入 `MemoryProbe` 模拟 RSS 越限 → 受害者收到
  `1013 Relay memory pressure`，压力期新升级拒绝，回落后恢复准入

### 9.3 握手校验测试（`tests/handshake_test.rs`，5 个 + 6 个单元）

- 合法 X25519 公钥通过并转发
- 非 Base64 / 非 32 字节 / 非域内坐标 / 弱公钥 → `1008`，且**不转发**
- `type` 非 hello/e2ee_hello → 透传，不校验
- server 角色帧不做握手校验

### 9.4 运维端点测试（`tests/operations_test.rs`，5 个）

- `/health` 始终 `200`（drain 中也是）
- drain 模式下 `/ready` 返回 `503`，取消 drain 后恢复；drain 中新会话 `503`
- `/metrics` 包含全部预期指标行（含 forwarded 计数正确性）
- 未知路径 `404`

### 9.5 尚未覆盖的场景

- **Supervisor 重启**：Capacity panic → listener 级联重启的端到端用例
  （重启路径本身已实现；`capacity_mutation_timeout` 触发链路需注入可控的
  wedge 手段才能稳定测试）
- **Writer deadline / Slow consumer**（`1013 Slow consumer`）与
  **control nudge → `1011 Control unresponsive`**：默认时长（30s / 10s+5s）
  过长，不适合集成测试；如需覆盖，应先把这几个时长做成可配置项

---

## 10. 工作量（实际）

| 模块 | 估算行数 | 实际行数（含注释与单元测试） |
|---|---|---|
| `config.rs` | ~200 | 261 |
| `protocol.rs` / `connection.rs` / `drain.rs` / `lib.rs` | ~200 | 214 |
| `handshake.rs` | ~100 | 191 |
| `metrics.rs` + `operations.rs` | ~500 | 364 |
| `ownership/mod.rs` + `owner.rs` | ~700 | 618 |
| `capacity/mod.rs` | ~800 | 706 |
| `delivery/writer.rs` + `delivery/mod.rs` | ~600 | 544 |
| `socket.rs` | ~600 | 442 |
| `main.rs` + `node.rs`（含 supervisor） | ~200 | 290 |
| **实现合计** | **~3,900** | **3,630** |
| 集成测试（tests/，含 common 工具） | ~2,000 | 712 |
| **总计** | **~5,900** | **4,342** |

行数估算基本命中；集成测试因公共工具收敛（`tests/common/mod.rs`）而明显少于
预估。原估工期 3–4 个月偏保守：实现 + 测试对齐的实际工作量对 1 名熟悉
async Rust 的工程师约 **1–2 个月**（含 9.5 节的补测与生产化收尾）。
release 二进制约 4.4 MB（thin LTO），无运行时依赖。

---

## 11. 不在范围内的事项

以下内容明确不实现，如未来需要可单独立项：

- **v1 协议**：返回 `400` 拒绝，不路由
- **分布式 ownership**：单节点，无跨节点 reroute
- **DNS 集群发现**：无 `PASEO_RELAY_CLUSTER_QUERY`
- **Fly.io 适配器**：无 `fly-replay` 支持
- **Docker / 容器化**：不含 Dockerfile
- **热重载 / zero-downtime 部署**：依赖进程管理器（systemd 等）的 graceful restart
- **WebSocket 压缩**：原实现关闭，保持一致
