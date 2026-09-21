use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tower_http::services::{ServeDir, ServeFile};

use crate::capacity::{self, CapacityConfig, CapacityHandle, MemoryProbe};
use crate::config::Config;
use crate::connection::Connection;
use crate::drain::Drain;
use crate::metrics::Metrics;
use crate::operations;
use crate::ownership::{Ownership, RouteResult};
use crate::socket::{self, Attached};
use crate::web;

pub struct AppState {
    pub config: Arc<Config>,
    pub ownership: Ownership,
    pub capacity: CapacityHandle,
    pub drain: Arc<Drain>,
    pub metrics: Arc<Metrics>,
}

pub struct NodeOptions {
    pub config: Config,
    pub drain: Arc<Drain>,
    pub metrics: Arc<Metrics>,
    pub memory_probe: Option<MemoryProbe>,
}

pub struct Node {
    pub local_addr: SocketAddr,
    /// Cancelling this token shuts the node down permanently.
    pub cancel: CancellationToken,
    /// The current generation's state; refreshed after a supervisor restart.
    pub state: watch::Receiver<Arc<AppState>>,
    pub done: tokio::task::JoinHandle<()>,
}

impl Node {
    pub fn current_state(&self) -> Arc<AppState> {
        self.state.borrow().clone()
    }
}

/// Bind the listener and run the supervisor loop: each generation is one
/// capacity actor plus one axum listener under a shared child token. When the
/// capacity actor dies (crash or wedged-mutation cancel), the listener is torn
/// down with it and both restart — the `rest_for_one` analog.
pub async fn start(options: NodeOptions) -> std::io::Result<Node> {
    let listener = TcpListener::bind((options.config.host, options.config.port)).await?;
    configure_listener(&listener, &options.config);
    let local_addr = listener.local_addr()?;
    let root_cancel = CancellationToken::new();

    let generation_cancel = root_cancel.child_token();
    let (state, capacity_done) = build_generation(&options, generation_cancel.clone());
    let (state_tx, state_rx) = watch::channel(state.clone());

    let done = tokio::spawn(supervisor_loop(
        listener,
        local_addr,
        options,
        root_cancel.clone(),
        state_tx,
        state,
        generation_cancel,
        capacity_done,
    ));

    Ok(Node {
        local_addr,
        cancel: root_cancel,
        state: state_rx,
        done,
    })
}

fn build_generation(
    options: &NodeOptions,
    cancel: CancellationToken,
) -> (Arc<AppState>, tokio::task::JoinHandle<()>) {
    let config = Arc::new(options.config.clone());
    let (capacity, capacity_done) = capacity::spawn(
        CapacityConfig {
            max_connections: config.max_connections,
            ingress_budget_bytes: config.ingress_budget_bytes,
            ingress_weight: config.ingress_weight,
            memory_watermark_bytes: config.memory_watermark_bytes,
        },
        Duration::from_millis(config.capacity_mutation_timeout_ms),
        options.metrics.clone(),
        options.memory_probe.clone(),
        cancel,
    );
    let state = Arc::new(AppState {
        config,
        ownership: Ownership::new(),
        capacity,
        drain: options.drain.clone(),
        metrics: options.metrics.clone(),
    });
    (state, capacity_done)
}

#[allow(clippy::too_many_arguments)]
async fn supervisor_loop(
    mut listener: TcpListener,
    local_addr: SocketAddr,
    options: NodeOptions,
    root_cancel: CancellationToken,
    state_tx: watch::Sender<Arc<AppState>>,
    mut state: Arc<AppState>,
    mut generation_cancel: CancellationToken,
    mut capacity_done: tokio::task::JoinHandle<()>,
) {
    loop {
        let mut router = Router::new()
            .route("/health", get(operations::health))
            .route("/ready", get(operations::ready))
            .route("/metrics", get(operations::metrics))
            .route("/ws", any(ws_handler))
            .with_state(state.clone());

        if let Some(dir) = web_ui_dir(&options.config) {
            let index = dir.join("index.html");
            router = router
                .fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
        } else {
            router = router
                .route("/", get(web::index))
                .route("/static/nacl-fast.js", get(web::nacl_fast))
                .fallback(operations::not_found);
        }

        let serve_cancel = generation_cancel.clone();
        let listener_done = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
                .await;
        });

        tokio::select! {
            _ = &mut capacity_done => generation_cancel.cancel(),
            _ = generation_cancel.cancelled() => {}
        }
        let _ = listener_done.await;

        if root_cancel.is_cancelled() {
            return;
        }

        tracing::error!("supervisor: restarting capacity + listener");
        tokio::time::sleep(Duration::from_millis(100)).await;

        listener = loop {
            match TcpListener::bind(local_addr).await {
                Ok(listener) => break listener,
                Err(error) => {
                    tracing::error!("supervisor: rebind failed: {error}");
                    if root_cancel.is_cancelled() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        };
        configure_listener(&listener, &options.config);

        generation_cancel = root_cancel.child_token();
        let (next_state, next_done) = build_generation(&options, generation_cancel.clone());
        state = next_state;
        capacity_done = next_done;
        let _ = state_tx.send(state.clone());
    }
}

fn web_ui_dir(config: &Config) -> Option<PathBuf> {
    let dir = config.web_ui_dir.as_ref()?;
    dir.join("index.html").is_file().then(|| dir.clone())
}

fn configure_listener(listener: &TcpListener, config: &Config) {
    let socket = socket2::SockRef::from(listener);
    let _ = socket.set_recv_buffer_size(config.tcp_receive_buffer_bytes);
}

async fn ws_handler(
    State(state): State<Arc<AppState>>,
    upgrade: WebSocketUpgrade,
    request: axum::http::Request<axum::body::Body>,
) -> Response {
    let query = request.uri().query().unwrap_or("");
    let connection = match Connection::from_query(query) {
        Ok(connection) => connection,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };

    let capacity_token = match state.capacity.admit_connection().await {
        Ok(token) => token,
        Err(error) => {
            Metrics::inc(&state.metrics.connection_rejections);
            let (status, message) = socket::admission_rejection(error);
            return (
                StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                message,
            )
                .into_response();
        }
    };

    let route = state
        .ownership
        .route(&connection.server_id, state.drain.draining())
        .await;
    let (owner, reservation) = match route {
        RouteResult::Local { owner, reservation } => (owner, reservation),
        RouteResult::Unavailable => {
            state.capacity.release_connection(capacity_token);
            return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
        }
    };

    let max_payload = if connection.is_control() {
        crate::protocol::MAXIMUM_CONTROL_PAYLOAD_BYTES
    } else {
        crate::protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES
    };

    upgrade
        .max_message_size(max_payload)
        .max_frame_size(crate::protocol::MAXIMUM_FRAME_WIRE_BYTES)
        .on_upgrade(move |websocket| {
            socket::handle_socket(
                websocket,
                Attached {
                    connection,
                    owner,
                    reservation,
                    capacity_token,
                },
                state,
            )
        })
}
