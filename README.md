# paseo-relay-rs

Single-node, protocol-compatible rewrite of [paseo-relay](https://github.com/getpaseo/paseo-relay) in
Rust (tokio + axum), implementing the plan in `doc/REWRITE.md`.

Daemons and clients meet by `serverId`; frames are end-to-end encrypted by the
Paseo protocol, so the relay never sees content. Only the **v2** protocol is
served — `v=1` (or a missing `v`) is rejected with `400 Invalid protocol
version`. Distributed ownership (Syn/cluster/reroute) and provider adapters
are intentionally out of scope.

## Build & test

```sh
cargo build --release
cargo test
```

## Run

```sh
PASEO_RELAY_PORT=4000 ./target/release/paseo-relay
```

WebSocket endpoint: `GET /ws?serverId=X&role=server|client&v=2[&connectionId=Y]`
Operations: `GET /health`, `GET /ready`, `GET /metrics` (Prometheus text).

## Web UI

The relay binary embeds the built Paseo web app (vendored under `web-ui/`) and
serves it at `/` with SPA fallback, so distribution stays a single binary. Open
an offer link directly:

```text
http://<relay-host>/#offer=<base64-offer>
```

To refresh the embedded app from a sibling `../paseo` checkout, run
`scripts/vendor-web-ui.sh` (or pass the export directory as an argument), then
rebuild.

An embedded debug pairing console can be enabled at `/debug` by setting
`PASEO_RELAY_DEBUG=true`; it parses the `#offer` fragment, connects to the
relay's `/ws` endpoint as a `role=client`, and performs the Paseo E2EE
handshake (NaCl box).

## Configuration

| Environment variable | Default | Meaning |
|---|---|---|
| `PASEO_RELAY_HOST` | `127.0.0.1` | Listen IP |
| `PASEO_RELAY_PORT` | `4000` | Listen port |
| `PASEO_RELAY_DRAIN` | `false` | Start in drain mode |
| `PASEO_RELAY_MAX_CONNECTIONS` | `20000` | Max active WebSockets |
| `PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS` | `15000` | Pre-upgrade HTTP idle timeout |
| `PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS` | `5000` | Capacity mutation timeout (timeout restarts the runtime) |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | Weighted ingress byte budget |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | Memory weight per ingress byte |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | Max wait per delivery |
| `PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS` | `35000` | TCP send timeout (must exceed delivery timeout) |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | Per-destination control queue cap |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | Client-frame wait for a data socket |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | Listener receive buffer |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | RSS watermark for shedding (0 = disabled) |
| `PASEO_RELAY_DEBUG` | `false` | Serve the embedded debug pairing console at `/debug` |

## Architecture

Every stateful component is a tokio actor (task + channel + oneshot replies):

- **Capacity** (`src/capacity/`) — serialized resource ledger: connection
  admission, weighted ingress budget, memory-watermark shedding. A wedged
  mutation cancels the runtime token; the supervisor loop in `src/node.rs`
  restarts capacity + listener together (`rest_for_one` analog).
- **Ownership/Owner** (`src/ownership/`) — one owner task per `serverId` in a
  `DashMap`: v2 routing table (control/data/clients), connection replacement
  (`1008`), disconnect cascades (`1001`/`1012`), attach-waiters with control
  nudge (`1011`), idle self-unregistration after 30 s.
- **Writer** (`src/delivery/writer.rs`) — per-destination write serializer
  with the write-barrier handshake (a frame completes only when the
  destination's sink has flushed it to TCP) and a bounded control queue
  (`1013 Slow consumer`).
- **Socket** (`src/socket.rs`) — connection lifecycle: one in-flight delivery
  at a time (`delivering` backpressure), handshake public-key validation for
  client frames (`1008 Invalid handshake key`), fail-closed guards that detach
  from the owner and release capacity on any exit path.

Close codes match the Elixir implementation; see section 2.9 of REWRITE.md.
