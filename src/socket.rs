use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::capacity::CapacityError;
use crate::connection::{Connection, Role};
use crate::delivery::writer::{self, WriterHandle};
use crate::delivery::{self};
use crate::handshake::{self, HandshakeResult};
use crate::next_id;
use crate::node::AppState;
use crate::ownership::owner::{Destinations, OwnerError, OwnerHandle};
use crate::protocol;

/// Messages other components push at a socket task (the Elixir
/// `websocket_info` surface).
pub enum SocketMsg {
    /// Forwarded frame from a writer.
    Frame { message: Message },
    /// Write barrier: acknowledged back to the writer once the frame before it
    /// has been flushed to TCP.
    Barrier {
        reference: u64,
        writer: WriterHandle,
    },
    /// Close the connection with an explicit code.
    Close { code: u16, reason: &'static str },
    /// Memory-pressure shed from the capacity actor.
    MemoryPressure,
}

/// Detaches from the owner and releases the capacity slot when the socket task
/// exits by any path (the `Process.monitor`/`terminate` analog).
struct SocketGuard {
    socket_id: u64,
    owner: OwnerHandle,
    state: Arc<AppState>,
    capacity_token: u64,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        self.owner.detach(self.socket_id);
        self.state.capacity.release_connection(self.capacity_token);
    }
}

pub struct Attached {
    pub connection: Connection,
    pub owner: OwnerHandle,
    pub reservation: u64,
    pub capacity_token: u64,
}

pub async fn handle_socket(websocket: WebSocket, attached: Attached, state: Arc<AppState>) {
    let socket_id = next_id();
    let (signal_tx, signal_rx) = mpsc::unbounded_channel::<SocketMsg>();

    if state
        .capacity
        .attach_connection(attached.capacity_token, socket_id, signal_tx.clone())
        .await
        .is_err()
    {
        let mut websocket = websocket;
        let _ = close(&mut websocket, 1012, "Session expired").await;
        state.capacity.release_connection(attached.capacity_token);
        return;
    }

    let (writer, writer_guard) = writer::spawn(
        signal_tx.clone(),
        Duration::from_millis(state.config.delivery_timeout_ms),
        state.config.control_queue_bytes,
        state.metrics.clone(),
    );

    let guard = SocketGuard {
        socket_id,
        owner: attached.owner.clone(),
        state: state.clone(),
        capacity_token: attached.capacity_token,
    };

    if attached
        .owner
        .attach(
            attached.reservation,
            socket_id,
            attached.connection.clone(),
            writer,
        )
        .await
        .is_err()
    {
        let mut websocket = websocket;
        let _ = close(&mut websocket, 1012, "Session expired").await;
        drop(writer_guard);
        drop(guard);
        return;
    }

    let (sink, stream) = websocket.split();
    run(
        socket_id,
        attached.connection,
        attached.owner,
        sink,
        stream,
        signal_rx,
        state,
    )
    .await;

    drop(writer_guard);
    drop(guard);
}

type Sink = SplitSink<WebSocket, Message>;
type Stream = SplitStream<WebSocket>;

enum DeliveryOutcome {
    Ok,
    AttachTimeout,
    Failed,
}

struct PendingFrame {
    message: Message,
    token: u64,
}

struct SocketTask {
    socket_id: u64,
    connection: Connection,
    owner: OwnerHandle,
    state: Arc<AppState>,
    sink: Sink,
    /// In-flight delivery: (result receiver, capacity message token).
    delivery: Option<(oneshot::Receiver<DeliveryOutcome>, u64)>,
    pending: VecDeque<PendingFrame>,
}

async fn run(
    socket_id: u64,
    connection: Connection,
    owner: OwnerHandle,
    sink: Sink,
    mut stream: Stream,
    mut signal_rx: mpsc::UnboundedReceiver<SocketMsg>,
    state: Arc<AppState>,
) {
    let mut task = SocketTask {
        socket_id,
        connection,
        owner,
        state,
        sink,
        delivery: None,
        pending: VecDeque::new(),
    };

    loop {
        let delivering = task.delivery.is_some();
        let delivery_done = async {
            match &mut task.delivery {
                Some((receiver, _token)) => match receiver.await {
                    Ok(outcome) => outcome,
                    Err(_closed) => DeliveryOutcome::Failed,
                },
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            // Frames and control signals pushed at this socket.
            signal = signal_rx.recv() => {
                let Some(signal) = signal else { break };
                match signal {
                    SocketMsg::Frame { message } => {
                        if task.sink.send(message).await.is_err() {
                            break;
                        }
                    }
                    SocketMsg::Barrier { reference, writer } => {
                        // The previous frame is flushed once flush() returns;
                        // then release the writer's active slot.
                        if task.sink.flush().await.is_err() {
                            break;
                        }
                        writer.acknowledge(reference);
                    }
                    SocketMsg::Close { code, reason } => {
                        let _ = close_sink(&mut task.sink, code, reason).await;
                        break;
                    }
                    SocketMsg::MemoryPressure => {
                        let _ = close_sink(&mut task.sink, 1013, "Relay memory pressure").await;
                        break;
                    }
                }
            }

            outcome = delivery_done => {
                let (_receiver, token) = task.delivery.take().unwrap();
                task.state.capacity.finish_message(token);
                match outcome {
                    DeliveryOutcome::Ok => {
                        if !task.continue_pending().await {
                            break;
                        }
                    }
                    DeliveryOutcome::AttachTimeout => {
                        let _ = close_sink(&mut task.sink, 1013, "Data route unavailable").await;
                        break;
                    }
                    DeliveryOutcome::Failed => {
                        let _ = close_sink(&mut task.sink, 1013, "Delivery unavailable").await;
                        break;
                    }
                }
            }

            // Backpressure: stop reading while a delivery is in flight.
            frame = stream.next(), if !delivering => {
                match frame {
                    Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => {
                        if !task.handle_frame(message).await {
                            break;
                        }
                    }
                    Some(Ok(_ping_pong)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }

    // Cancel any in-flight delivery reservation.
    if let Some((_receiver, token)) = task.delivery.take() {
        task.state.capacity.cancel_message(token);
    }
    for frame in task.pending.drain(..) {
        task.state.capacity.cancel_message(frame.token);
    }
}

impl SocketTask {
    /// Returns false when the socket should close.
    async fn handle_frame(&mut self, message: Message) -> bool {
        let payload_bytes = protocol::payload_len(&message);
        self.state.metrics.observe_frame(payload_bytes);

        if payload_bytes > self.max_payload() {
            let _ = close_sink(&mut self.sink, 1009, "Frame too large").await;
            return false;
        }

        if self.connection.is_control() {
            return self.handle_control_frame(message).await;
        }

        let token = match self
            .state
            .capacity
            .admit_message(self.socket_id, payload_bytes)
            .await
        {
            Ok(token) => token,
            Err(_) => {
                let _ = close_sink(&mut self.sink, 1013, "Relay ingress capacity").await;
                return false;
            }
        };

        // Handshake validation applies to client frames only.
        if self.connection.role == Role::Client {
            let payload: &[u8] = match &message {
                Message::Text(text) => text.as_bytes(),
                Message::Binary(bytes) => bytes,
                _ => &[],
            };
            match handshake::check(payload) {
                HandshakeResult::NotHandshake => {}
                HandshakeResult::Accept(handshake_type) => {
                    self.state.metrics.observe_handshake(true, handshake_type);
                }
                HandshakeResult::Reject(handshake_type) => {
                    self.state.metrics.observe_handshake(false, handshake_type);
                    self.state.capacity.finish_message(token);
                    let _ = close_sink(&mut self.sink, 1008, "Invalid handshake key").await;
                    return false;
                }
            }
        }

        self.start_delivery(message, token).await
    }

    /// Returns false when the socket should close.
    async fn start_delivery(&mut self, message: Message, token: u64) -> bool {
        if self
            .state
            .capacity
            .start_delivery(token, self.socket_id)
            .await
            .is_err()
        {
            self.state.capacity.cancel_message(token);
            let _ = close_sink(&mut self.sink, 1013, "Relay memory pressure").await;
            return false;
        }

        let (result_tx, result_rx) = oneshot::channel();
        let owner = self.owner.clone();
        let socket_id = self.socket_id;
        let deadline =
            Instant::now() + Duration::from_millis(self.state.config.delivery_timeout_ms);
        let attach_timeout = Duration::from_millis(self.state.config.data_attach_timeout_ms);

        tokio::spawn(async move {
            let outcome = match owner
                .destinations(socket_id, deadline, attach_timeout)
                .await
            {
                Ok(Destinations::Writers(writers)) => {
                    match delivery::deliver(writers, message, deadline).await {
                        Ok(()) => DeliveryOutcome::Ok,
                        Err(_error) => DeliveryOutcome::Failed,
                    }
                }
                Ok(Destinations::Control) => DeliveryOutcome::Ok,
                Err(OwnerError::AttachTimeout) => DeliveryOutcome::AttachTimeout,
                Err(_error) => DeliveryOutcome::Failed,
            };
            let _ = result_tx.send(outcome);
        });

        self.delivery = Some((result_rx, token));
        true
    }

    /// Returns false when the socket should close.
    async fn continue_pending(&mut self) -> bool {
        if let Some(frame) = self.pending.pop_front() {
            self.start_delivery(frame.message, frame.token).await
        } else {
            true
        }
    }

    /// Control socket input: only `{"type":"ping"}` is understood; everything
    /// else is dropped. Nothing is forwarded.
    async fn handle_control_frame(&mut self, message: Message) -> bool {
        let payload_bytes = protocol::payload_len(&message);
        let Message::Text(text) = message else {
            return true;
        };
        let token = match self
            .state
            .capacity
            .admit_message(self.socket_id, payload_bytes)
            .await
        {
            Ok(token) => token,
            Err(_) => {
                let _ = close_sink(&mut self.sink, 1013, "Relay ingress capacity").await;
                return false;
            }
        };

        let keep_open = match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(value) if value.get("type").and_then(|t| t.as_str()) == Some("ping") => {
                let pong = serde_json::json!({
                    "type": "pong",
                    "ts": unix_millis(),
                })
                .to_string();
                match self.owner.control(self.socket_id, pong).await {
                    Ok(()) => true,
                    Err(_error) => {
                        let _ = close_sink(&mut self.sink, 1013, "Delivery unavailable").await;
                        false
                    }
                }
            }
            _not_ping => true,
        };
        self.state.capacity.finish_message(token);
        keep_open
    }

    fn max_payload(&self) -> usize {
        if self.connection.is_control() {
            protocol::MAXIMUM_CONTROL_PAYLOAD_BYTES
        } else {
            protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES
        }
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

async fn close(
    websocket: &mut WebSocket,
    code: u16,
    reason: &'static str,
) -> Result<(), axum::Error> {
    websocket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: Utf8Bytes::from_static(reason),
        })))
        .await
}

async fn close_sink(sink: &mut Sink, code: u16, reason: &'static str) -> Result<(), axum::Error> {
    sink.send(Message::Close(Some(CloseFrame {
        code,
        reason: Utf8Bytes::from_static(reason),
    })))
    .await
}

/// Map capacity admission errors to upgrade-time HTTP rejections.
pub fn admission_rejection(error: CapacityError) -> (u16, &'static str) {
    match error {
        CapacityError::Capacity => (503, "Relay connection capacity"),
        CapacityError::Pressure => (503, "Relay memory pressure"),
        _unavailable => (503, "Relay capacity unavailable"),
    }
}
