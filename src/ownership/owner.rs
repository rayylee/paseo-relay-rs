use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::connection::{Connection, Role};
use crate::delivery::writer::WriterHandle;
use crate::next_id;

const RESERVATION_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const NUDGE_DELAY: Duration = Duration::from_secs(10);
const RESET_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerError {
    Closed,
    Detached,
    AttachTimeout,
    OwnerTimeout,
}

/// Destinations for one inbound frame.
pub enum Destinations {
    Writers(Vec<WriterHandle>),
    /// Control-socket input is not forwarded.
    Control,
}

enum Msg {
    Reserve {
        reply: oneshot::Sender<u64>,
    },
    Attach {
        reservation: u64,
        socket_id: u64,
        connection: Connection,
        writer: WriterHandle,
        reply: oneshot::Sender<Result<(), OwnerError>>,
    },
    Detach {
        socket_id: u64,
    },
    Destinations {
        socket_id: u64,
        deadline: Instant,
        attach_timeout: Duration,
        reply: oneshot::Sender<Result<Destinations, OwnerError>>,
    },
    Control {
        socket_id: u64,
        payload: String,
        reply: oneshot::Sender<Result<(), OwnerError>>,
    },
    Expired {
        reservation: u64,
    },
    DataTimeout {
        wait_token: u64,
    },
    NudgeControl {
        connection_id: String,
    },
    ResetControl {
        connection_id: String,
    },
    IdleCheck,
}

#[derive(Clone)]
pub struct OwnerHandle {
    sender: mpsc::UnboundedSender<Msg>,
}

impl OwnerHandle {
    pub async fn reserve(&self) -> Result<u64, OwnerError> {
        self.call(|reply| Msg::Reserve { reply }).await
    }

    pub async fn attach(
        &self,
        reservation: u64,
        socket_id: u64,
        connection: Connection,
        writer: WriterHandle,
    ) -> Result<(), OwnerError> {
        self.call(|reply| Msg::Attach {
            reservation,
            socket_id,
            connection,
            writer,
            reply,
        })
        .await?
    }

    pub fn detach(&self, socket_id: u64) {
        let _ = self.sender.send(Msg::Detach { socket_id });
    }

    /// Resolve the destinations for a frame from `socket_id`. For clients whose
    /// data socket has not attached yet, the reply is parked until the data
    /// socket arrives or `min(deadline, attach_timeout)` elapses.
    pub async fn destinations(
        &self,
        socket_id: u64,
        deadline: Instant,
        attach_timeout: Duration,
    ) -> Result<Destinations, OwnerError> {
        if Instant::now() >= deadline {
            return Err(OwnerError::OwnerTimeout);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Msg::Destinations {
                socket_id,
                deadline,
                attach_timeout,
                reply: reply_tx,
            })
            .map_err(|_| OwnerError::Closed)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_closed)) => Err(OwnerError::Closed),
            Err(_timeout) => Err(OwnerError::OwnerTimeout),
        }
    }

    pub async fn control(&self, socket_id: u64, payload: String) -> Result<(), OwnerError> {
        self.call(|reply| Msg::Control {
            socket_id,
            payload,
            reply,
        })
        .await?
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> Msg,
    ) -> Result<T, OwnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(build(reply_tx))
            .map_err(|_| OwnerError::Closed)?;
        match tokio::time::timeout(CALL_TIMEOUT, reply_rx).await {
            Ok(Ok(value)) => Ok(value),
            _closed_or_timeout => Err(OwnerError::Closed),
        }
    }
}

struct SocketEntry {
    connection: Connection,
    writer: WriterHandle,
}

struct Waiter {
    source: u64,
    connection_id: String,
    reply: oneshot::Sender<Result<Destinations, OwnerError>>,
}

struct OwnerState {
    server_id: String,
    owners: Arc<DashMap<String, OwnerHandle>>,
    sender: mpsc::UnboundedSender<Msg>,
    reservations: HashMap<u64, Instant>,
    sockets: HashMap<u64, SocketEntry>,
    control: Option<u64>,
    data: HashMap<String, u64>,
    clients: HashMap<String, HashSet<u64>>,
    waiting: HashMap<u64, Waiter>,
    idle_since: Option<Instant>,
}

pub fn spawn(server_id: String, owners: Arc<DashMap<String, OwnerHandle>>) -> OwnerHandle {
    let (sender, receiver) = mpsc::unbounded_channel();
    let handle = OwnerHandle {
        sender: sender.clone(),
    };
    let state = OwnerState {
        server_id,
        owners,
        sender,
        reservations: HashMap::new(),
        sockets: HashMap::new(),
        control: None,
        data: HashMap::new(),
        clients: HashMap::new(),
        waiting: HashMap::new(),
        idle_since: None,
    };
    tokio::spawn(run(state, receiver));
    handle
}

async fn run(mut state: OwnerState, mut receiver: mpsc::UnboundedReceiver<Msg>) {
    while let Some(message) = receiver.recv().await {
        if !state.handle(message) {
            break;
        }
    }
    // Unregister; guard against a newer owner having already taken the slot.
    state.owners.remove_if(&state.server_id, |_key, handle| {
        handle.sender.same_channel(&state.sender)
    });
}

impl OwnerState {
    fn send_after(&self, delay: Duration, message: impl FnOnce() -> Msg) {
        let sender = self.sender.clone();
        let message = message();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = sender.send(message);
        });
    }

    /// Returns false when the owner should stop.
    fn handle(&mut self, message: Msg) -> bool {
        match message {
            Msg::Reserve { reply } => {
                let token = next_id();
                self.idle_since = None;
                self.reservations
                    .insert(token, Instant::now() + RESERVATION_TIMEOUT);
                self.send_after(RESERVATION_TIMEOUT, || Msg::Expired { reservation: token });
                let _ = reply.send(token);
                true
            }
            Msg::Attach {
                reservation,
                socket_id,
                connection,
                writer,
                reply,
            } => {
                match self.reservations.remove(&reservation) {
                    Some(expires_at) if expires_at > Instant::now() => {
                        self.idle_since = None;
                        self.sockets.insert(
                            socket_id,
                            SocketEntry {
                                connection: connection.clone(),
                                writer,
                            },
                        );
                        self.attach_connection(socket_id, &connection);
                        let _ = reply.send(Ok(()));
                    }
                    _missing_or_expired => {
                        let _ = reply.send(Err(OwnerError::Closed));
                    }
                }
                true
            }
            Msg::Detach { socket_id } => {
                self.remove_socket(socket_id);
                self.check_idle()
            }
            Msg::Destinations {
                socket_id,
                deadline,
                attach_timeout,
                reply,
            } => {
                self.destinations(socket_id, deadline, attach_timeout, reply);
                true
            }
            Msg::Control {
                socket_id,
                payload,
                reply,
            } => {
                match self.sockets.get(&socket_id) {
                    None => {
                        let _ = reply.send(Err(OwnerError::Detached));
                    }
                    Some(entry) => {
                        let result = entry
                            .writer
                            .control(payload)
                            .map(|_pending| ())
                            .map_err(|_| OwnerError::Closed);
                        let _ = reply.send(result);
                    }
                }
                true
            }
            Msg::Expired { reservation } => {
                if let Some(expires_at) = self.reservations.get(&reservation) {
                    if *expires_at <= Instant::now() {
                        self.reservations.remove(&reservation);
                    }
                }
                self.check_idle()
            }
            Msg::DataTimeout { wait_token } => {
                if let Some(waiter) = self.waiting.remove(&wait_token) {
                    let _ = waiter.reply.send(Err(OwnerError::AttachTimeout));
                }
                true
            }
            Msg::NudgeControl { connection_id } => {
                if self.waiting_for_data(&connection_id) {
                    let connection_ids: Vec<&String> = self.clients.keys().collect();
                    self.notify_control(&serde_json::json!({
                        "type": "sync",
                        "connectionIds": connection_ids,
                    }));
                    self.send_after(RESET_DELAY, || Msg::ResetControl { connection_id });
                }
                true
            }
            Msg::ResetControl { connection_id } => {
                if self.waiting_for_data(&connection_id) {
                    if let Some(control) = self.control {
                        self.close(control, 1011, "Control unresponsive");
                    }
                }
                true
            }
            Msg::IdleCheck => {
                !(self.empty()
                    && self
                        .idle_since
                        .is_some_and(|since| since.elapsed() >= IDLE_TIMEOUT))
            }
        }
    }

    fn empty(&self) -> bool {
        self.reservations.is_empty() && self.sockets.is_empty()
    }

    /// Returns false when the owner should stop (idle immediately after the
    /// grace period is unnecessary here — schedule the check instead).
    fn check_idle(&mut self) -> bool {
        if self.empty() && self.idle_since.is_none() {
            self.idle_since = Some(Instant::now());
            self.send_after(IDLE_TIMEOUT, || Msg::IdleCheck);
        }
        true
    }

    fn attach_connection(&mut self, socket_id: u64, connection: &Connection) {
        match (connection.role, connection.connection_id.as_str()) {
            (Role::Server, "") => {
                if let Some(old) = self.control.replace(socket_id) {
                    self.close(old, 1008, "Replaced by new connection");
                }
                let connection_ids: Vec<&String> = self.clients.keys().collect();
                self.notify(
                    socket_id,
                    &serde_json::json!({
                        "type": "sync",
                        "connectionIds": connection_ids,
                    }),
                );
            }
            (Role::Server, connection_id) => {
                if let Some(old) = self.data.insert(connection_id.to_string(), socket_id) {
                    self.close(old, 1008, "Replaced by new connection");
                }
                self.release_waiters(connection_id, socket_id);
            }
            (Role::Client, connection_id) => {
                self.clients
                    .entry(connection_id.to_string())
                    .or_default()
                    .insert(socket_id);
                self.notify_control(&serde_json::json!({
                    "type": "connected",
                    "connectionId": connection_id,
                }));
                let connection_id = connection_id.to_string();
                self.send_after(NUDGE_DELAY, || Msg::NudgeControl { connection_id });
            }
        }
    }

    fn remove_socket(&mut self, socket_id: u64) {
        let Some(entry) = self.sockets.remove(&socket_id) else {
            return;
        };
        self.detach_connection(socket_id, &entry.connection);
        self.reject_source_waiters(socket_id);
    }

    fn detach_connection(&mut self, socket_id: u64, connection: &Connection) {
        match (connection.role, connection.connection_id.as_str()) {
            (Role::Server, "") => {
                if self.control == Some(socket_id) {
                    self.control = None;
                }
            }
            (Role::Server, connection_id) => {
                if self.data.get(connection_id) == Some(&socket_id) {
                    self.data.remove(connection_id);
                    for client in self
                        .clients
                        .get(connection_id)
                        .map(|set| set.iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default()
                    {
                        self.close(client, 1012, "Server disconnected");
                    }
                }
            }
            (Role::Client, connection_id) => {
                let remaining = if let Some(set) = self.clients.get_mut(connection_id) {
                    set.remove(&socket_id);
                    set.len()
                } else {
                    return;
                };
                if remaining == 0 {
                    self.clients.remove(connection_id);
                    if let Some(&data_socket) = self.data.get(connection_id) {
                        self.close(data_socket, 1001, "Client disconnected");
                    }
                    self.notify_control(&serde_json::json!({
                        "type": "disconnected",
                        "connectionId": connection_id,
                    }));
                }
            }
        }
    }

    fn destinations(
        &mut self,
        socket_id: u64,
        deadline: Instant,
        attach_timeout: Duration,
        reply: oneshot::Sender<Result<Destinations, OwnerError>>,
    ) {
        let Some(entry) = self.sockets.get(&socket_id) else {
            let _ = reply.send(Err(OwnerError::Detached));
            return;
        };
        match (
            entry.connection.role,
            entry.connection.connection_id.as_str(),
        ) {
            (Role::Server, "") => {
                let _ = reply.send(Ok(Destinations::Control));
            }
            (Role::Server, connection_id) => {
                let writers = self.writers(
                    self.clients
                        .get(connection_id)
                        .map(|set| set.iter().copied().collect())
                        .unwrap_or_default(),
                );
                let _ = reply.send(Ok(Destinations::Writers(writers)));
            }
            (Role::Client, connection_id) => match self.data.get(connection_id) {
                Some(&target) => {
                    let writers = self.writers(vec![target]);
                    let _ = reply.send(Ok(Destinations::Writers(writers)));
                }
                None => {
                    let timeout =
                        attach_timeout.min(deadline.saturating_duration_since(Instant::now()));
                    let wait_token = next_id();
                    self.waiting.insert(
                        wait_token,
                        Waiter {
                            source: socket_id,
                            connection_id: connection_id.to_string(),
                            reply,
                        },
                    );
                    self.send_after(timeout, || Msg::DataTimeout { wait_token });
                }
            },
        }
    }

    fn release_waiters(&mut self, connection_id: &str, data_socket: u64) {
        let tokens: Vec<u64> = self
            .waiting
            .iter()
            .filter(|(_token, waiter)| waiter.connection_id == connection_id)
            .map(|(token, _waiter)| *token)
            .collect();
        for token in tokens {
            let waiter = self.waiting.remove(&token).unwrap();
            let writers = self.writers(vec![data_socket]);
            let _ = waiter.reply.send(Ok(Destinations::Writers(writers)));
        }
    }

    fn reject_source_waiters(&mut self, source: u64) {
        let tokens: Vec<u64> = self
            .waiting
            .iter()
            .filter(|(_token, waiter)| waiter.source == source)
            .map(|(token, _waiter)| *token)
            .collect();
        for token in tokens {
            let waiter = self.waiting.remove(&token).unwrap();
            let _ = waiter.reply.send(Err(OwnerError::Detached));
        }
    }

    fn writers(&self, socket_ids: Vec<u64>) -> Vec<WriterHandle> {
        socket_ids
            .into_iter()
            .filter_map(|socket_id| {
                self.sockets
                    .get(&socket_id)
                    .map(|entry| entry.writer.clone())
            })
            .collect()
    }

    fn waiting_for_data(&self, connection_id: &str) -> bool {
        self.clients.contains_key(connection_id) && !self.data.contains_key(connection_id)
    }

    fn close(&self, socket_id: u64, code: u16, reason: &'static str) {
        if let Some(entry) = self.sockets.get(&socket_id) {
            entry.writer.close(code, reason);
        }
    }

    fn notify_control(&self, message: &serde_json::Value) {
        if let Some(control) = self.control {
            self.notify(control, message);
        }
    }

    fn notify(&self, socket_id: u64, message: &serde_json::Value) {
        if let Some(entry) = self.sockets.get(&socket_id) {
            let _ = entry.writer.control(message.to_string());
        }
    }
}
