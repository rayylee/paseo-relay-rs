use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::metrics::{self, CapacityGauges, Metrics};
use crate::next_id;
use crate::protocol;
use crate::socket::SocketMsg;

const RESERVATION_TIMEOUT: Duration = Duration::from_secs(5);
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(1);
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const PRESSURE_RECHECK: Duration = Duration::from_millis(100);
const INITIAL_MAX_SHED_BATCH: usize = 64;
const MAX_SHED_BATCH: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityError {
    Unavailable,
    Pressure,
    Capacity,
    Closed,
    Expired,
    MessageExceedsBudget,
    BudgetExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Open,
    Pressure,
    ConnectionCapacity,
}

#[derive(Debug, Clone, Copy)]
pub struct CapacityStatus {
    pub admission: Admission,
    pub gauges: CapacityGauges,
}

pub struct CapacityConfig {
    pub max_connections: usize,
    pub ingress_budget_bytes: usize,
    pub ingress_weight: usize,
    pub memory_watermark_bytes: usize,
}

pub type MemoryProbe = Arc<dyn Fn() -> u64 + Send + Sync>;

enum Msg {
    AdmitConnection {
        reply: oneshot::Sender<Result<u64, CapacityError>>,
    },
    AttachConnection {
        token: u64,
        socket_id: u64,
        signal: mpsc::UnboundedSender<SocketMsg>,
        reply: oneshot::Sender<Result<(), CapacityError>>,
    },
    ReleaseConnection {
        token: u64,
    },
    AdmitMessage {
        socket_id: u64,
        payload_bytes: usize,
        reply: oneshot::Sender<Result<u64, CapacityError>>,
    },
    StartDelivery {
        token: u64,
        socket_id: u64,
        reply: oneshot::Sender<Result<(), CapacityError>>,
    },
    FinishMessage {
        token: u64,
        observe_wait: bool,
    },
    Status {
        reply: oneshot::Sender<CapacityStatus>,
    },
    CheckNow {
        reply: oneshot::Sender<()>,
    },
    SetWatermark {
        bytes: usize,
        reply: oneshot::Sender<()>,
    },
}

/// Cheap-to-clone handle to the capacity actor. Mutations carry the configured
/// timeout; a timeout cancels the runtime token, which restarts the capacity
/// actor and listener together (the `rest_for_one` analog).
#[derive(Clone)]
pub struct CapacityHandle {
    sender: mpsc::Sender<Msg>,
    cancel: CancellationToken,
    mutation_timeout: Duration,
}

impl CapacityHandle {
    pub async fn admit_connection(&self) -> Result<u64, CapacityError> {
        self.mutate(|reply| Msg::AdmitConnection { reply }).await?
    }

    pub async fn attach_connection(
        &self,
        token: u64,
        socket_id: u64,
        signal: mpsc::UnboundedSender<SocketMsg>,
    ) -> Result<(), CapacityError> {
        self.mutate(|reply| Msg::AttachConnection {
            token,
            socket_id,
            signal,
            reply,
        })
        .await?
    }

    pub fn release_connection(&self, token: u64) {
        let _ = self.sender.try_send(Msg::ReleaseConnection { token });
    }

    pub async fn admit_message(
        &self,
        socket_id: u64,
        payload_bytes: usize,
    ) -> Result<u64, CapacityError> {
        self.mutate(|reply| Msg::AdmitMessage {
            socket_id,
            payload_bytes,
            reply,
        })
        .await?
    }

    pub async fn start_delivery(&self, token: u64, socket_id: u64) -> Result<(), CapacityError> {
        self.mutate(|reply| Msg::StartDelivery {
            token,
            socket_id,
            reply,
        })
        .await?
    }

    pub fn finish_message(&self, token: u64) {
        let _ = self.sender.try_send(Msg::FinishMessage {
            token,
            observe_wait: true,
        });
    }

    pub fn cancel_message(&self, token: u64) {
        let _ = self.sender.try_send(Msg::FinishMessage {
            token,
            observe_wait: false,
        });
    }

    pub async fn status(&self) -> Option<CapacityStatus> {
        self.observe(|reply| Msg::Status { reply }).await
    }

    pub async fn check_now(&self) {
        let _ = self.observe(|reply| Msg::CheckNow { reply }).await;
    }

    pub async fn set_watermark(&self, bytes: usize) {
        let _ = self
            .observe(|reply| Msg::SetWatermark { bytes, reply })
            .await;
    }

    /// Send a mutation and wait for the reply. On timeout the capacity actor
    /// is considered wedged: cancel the runtime so the supervisor restarts it
    /// (equivalent to `Process.exit(capacity, :kill)` under `rest_for_one`).
    async fn mutate<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> Msg,
    ) -> Result<T, CapacityError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.sender.try_send(build(reply_tx)).is_err() {
            return Err(CapacityError::Unavailable);
        }
        match tokio::time::timeout(self.mutation_timeout, reply_rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_closed)) => Err(CapacityError::Unavailable),
            Err(_timeout) => {
                self.cancel.cancel();
                Err(CapacityError::Unavailable)
            }
        }
    }

    async fn observe<T>(&self, build: impl FnOnce(oneshot::Sender<T>) -> Msg) -> Option<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.sender.try_send(build(reply_tx)).is_err() {
            return None;
        }
        match tokio::time::timeout(OBSERVATION_TIMEOUT, reply_rx).await {
            Ok(Ok(value)) => Some(value),
            _ => None,
        }
    }
}

enum ConnStatus {
    Reservation { expires_at: Instant },
    Active { socket_id: u64 },
}

struct SocketCapState {
    connection_token: u64,
    messages: HashSet<u64>,
    active_key: Option<(Instant, u64)>,
    blocked_key: Option<(Instant, u64)>,
    shedding: bool,
    signal: mpsc::UnboundedSender<SocketMsg>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MsgStatus {
    Reserved,
    Delivering,
}

struct MsgEntry {
    socket_id: u64,
    payload_bytes: usize,
    weighted_bytes: usize,
    status: MsgStatus,
    started: Option<Instant>,
}

struct PressureState {
    memory: u64,
    victims: usize,
    batch: usize,
}

struct CapacityState {
    max_connections: usize,
    ingress_limit: usize,
    ingress_weight: usize,
    watermark: usize,
    connections: HashMap<u64, ConnStatus>,
    sockets: HashMap<u64, SocketCapState>,
    messages: HashMap<u64, MsgEntry>,
    active: BTreeMap<(Instant, u64), u64>,
    blocked: BTreeMap<(Instant, u64), u64>,
    active_reservations: usize,
    active_websockets: usize,
    reserved_bytes: usize,
    inflight_bytes: usize,
    blocked_sources: usize,
    pressure: Option<PressureState>,
    pressure_recheck: bool,
    metrics: Arc<Metrics>,
    memory_probe: MemoryProbe,
}

pub fn spawn(
    config: CapacityConfig,
    mutation_timeout: Duration,
    metrics: Arc<Metrics>,
    memory_probe: Option<MemoryProbe>,
    cancel: CancellationToken,
) -> (CapacityHandle, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(4096);
    let state = CapacityState {
        max_connections: config.max_connections,
        ingress_limit: config.ingress_budget_bytes,
        ingress_weight: config.ingress_weight,
        watermark: config.memory_watermark_bytes,
        connections: HashMap::new(),
        sockets: HashMap::new(),
        messages: HashMap::new(),
        active: BTreeMap::new(),
        blocked: BTreeMap::new(),
        active_reservations: 0,
        active_websockets: 0,
        reserved_bytes: 0,
        inflight_bytes: 0,
        blocked_sources: 0,
        pressure: None,
        pressure_recheck: false,
        metrics,
        memory_probe: memory_probe.unwrap_or_else(|| Arc::new(metrics::process_rss_bytes)),
    };
    let handle = CapacityHandle {
        sender,
        cancel: cancel.clone(),
        mutation_timeout,
    };
    let done = tokio::spawn(run(state, receiver, cancel));
    (handle, done)
}

async fn run(
    mut state: CapacityState,
    mut receiver: mpsc::Receiver<Msg>,
    cancel: CancellationToken,
) {
    let mut check = tokio::time::interval(CHECK_INTERVAL);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut recheck_at: Option<Instant> = None;

    loop {
        let recheck = async {
            match recheck_at {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => return,
            message = receiver.recv() => {
                let Some(message) = message else { return };
                state.handle(message);
            }
            _ = check.tick() => {
                state.expire_reservations();
                state.shed_if_needed();
                recheck_at = state.take_pressure_recheck();
            }
            _ = recheck => {
                state.pressure_recheck = false;
                state.shed_if_needed();
                recheck_at = state.take_pressure_recheck();
            }
        }
        if recheck_at.is_none() {
            recheck_at = state.take_pressure_recheck();
        }
    }
}

impl CapacityState {
    fn handle(&mut self, message: Msg) {
        match message {
            Msg::AdmitConnection { reply } => {
                let _ = reply.send(self.admit_connection());
            }
            Msg::AttachConnection {
                token,
                socket_id,
                signal,
                reply,
            } => {
                let _ = reply.send(self.attach_connection(token, socket_id, signal));
            }
            Msg::ReleaseConnection { token } => self.release_connection(token),
            Msg::AdmitMessage {
                socket_id,
                payload_bytes,
                reply,
            } => {
                let _ = reply.send(self.admit_message(socket_id, payload_bytes));
            }
            Msg::StartDelivery {
                token,
                socket_id,
                reply,
            } => {
                let _ = reply.send(self.start_delivery(token, socket_id));
            }
            Msg::FinishMessage {
                token,
                observe_wait,
            } => self.remove_message(token, observe_wait),
            Msg::Status { reply } => {
                let _ = reply.send(CapacityStatus {
                    admission: self.admission(),
                    gauges: self.gauges(),
                });
            }
            Msg::CheckNow { reply } => {
                self.expire_reservations();
                self.shed_if_needed();
                let _ = reply.send(());
            }
            Msg::SetWatermark { bytes, reply } => {
                self.watermark = bytes;
                if bytes == 0 {
                    self.pressure = None;
                }
                let _ = reply.send(());
            }
        }
    }

    fn admission(&self) -> Admission {
        if self.pressure.is_some() {
            Admission::Pressure
        } else if self.active_connections() >= self.max_connections {
            Admission::ConnectionCapacity
        } else {
            Admission::Open
        }
    }

    fn active_connections(&self) -> usize {
        self.active_reservations + self.active_websockets
    }

    fn admit_connection(&mut self) -> Result<u64, CapacityError> {
        if self.pressure.is_some() {
            return Err(CapacityError::Pressure);
        }
        if self.active_connections() >= self.max_connections {
            return Err(CapacityError::Capacity);
        }
        let token = next_id();
        self.connections.insert(
            token,
            ConnStatus::Reservation {
                expires_at: Instant::now() + RESERVATION_TIMEOUT,
            },
        );
        self.active_reservations += 1;
        Ok(token)
    }

    fn attach_connection(
        &mut self,
        token: u64,
        socket_id: u64,
        signal: mpsc::UnboundedSender<SocketMsg>,
    ) -> Result<(), CapacityError> {
        match self.connections.get(&token) {
            Some(ConnStatus::Reservation { expires_at }) if *expires_at > Instant::now() => {}
            _missing_expired_or_attached => return Err(CapacityError::Expired),
        }
        let active_key = self.next_key();
        self.connections
            .insert(token, ConnStatus::Active { socket_id });
        self.sockets.insert(
            socket_id,
            SocketCapState {
                connection_token: token,
                messages: HashSet::new(),
                active_key: Some(active_key),
                blocked_key: None,
                shedding: false,
                signal,
            },
        );
        self.active.insert(active_key, socket_id);
        self.active_reservations -= 1;
        self.active_websockets += 1;
        Ok(())
    }

    fn release_connection(&mut self, token: u64) {
        match self.connections.get(&token) {
            None => {}
            Some(ConnStatus::Reservation { .. }) => {
                self.connections.remove(&token);
                self.active_reservations -= 1;
            }
            Some(ConnStatus::Active { socket_id }) => self.remove_socket(*socket_id),
        }
    }

    fn remove_socket(&mut self, socket_id: u64) {
        let Some(socket_state) = self.sockets.get(&socket_id) else {
            return;
        };
        let tokens: Vec<u64> = socket_state.messages.iter().copied().collect();
        for token in tokens {
            self.remove_message(token, false);
        }
        let Some(socket_state) = self.sockets.remove(&socket_id) else {
            return;
        };
        if let Some(key) = socket_state.active_key {
            self.active.remove(&key);
        }
        if let Some(key) = socket_state.blocked_key {
            self.blocked.remove(&key);
        }
        self.connections.remove(&socket_state.connection_token);
        self.active_websockets -= 1;
    }

    fn admit_message(
        &mut self,
        socket_id: u64,
        payload_bytes: usize,
    ) -> Result<u64, CapacityError> {
        let weighted_bytes = payload_bytes * self.ingress_weight;
        let Some(socket_state) = self.sockets.get_mut(&socket_id) else {
            return Err(CapacityError::Closed);
        };
        if socket_state.shedding {
            return Err(CapacityError::Closed);
        }
        if self.pressure.is_some() {
            return Err(CapacityError::Pressure);
        }
        if weighted_bytes > self.ingress_limit {
            return Err(CapacityError::MessageExceedsBudget);
        }
        if self.reserved_bytes + weighted_bytes > self.ingress_limit {
            return Err(CapacityError::BudgetExhausted);
        }
        let token = next_id();
        socket_state.messages.insert(token);
        self.messages.insert(
            token,
            MsgEntry {
                socket_id,
                payload_bytes,
                weighted_bytes,
                status: MsgStatus::Reserved,
                started: None,
            },
        );
        self.reserved_bytes += weighted_bytes;
        Ok(token)
    }

    fn start_delivery(&mut self, token: u64, socket_id: u64) -> Result<(), CapacityError> {
        match self.messages.get(&token) {
            Some(message)
                if message.status == MsgStatus::Reserved && message.socket_id == socket_id => {}
            _missing_or_started => return Err(CapacityError::Expired),
        }
        let Some(socket_state) = self.sockets.get(&socket_id) else {
            return Err(CapacityError::Closed);
        };
        if socket_state.shedding {
            return Err(CapacityError::Closed);
        }
        if self.pressure.is_some() {
            return Err(CapacityError::Pressure);
        }
        if socket_state.blocked_key.is_none() {
            let key = self.next_key();
            self.blocked.insert(key, socket_id);
            self.blocked_sources += 1;
            self.sockets.get_mut(&socket_id).unwrap().blocked_key = Some(key);
        }
        let message = self.messages.get_mut(&token).unwrap();
        message.status = MsgStatus::Delivering;
        message.started = Some(Instant::now());
        self.inflight_bytes += message.payload_bytes;
        Ok(())
    }

    fn remove_message(&mut self, token: u64, observe_wait: bool) {
        let Some(message) = self.messages.remove(&token) else {
            return;
        };
        if observe_wait && message.status == MsgStatus::Delivering {
            if let Some(started) = message.started {
                self.metrics.observe_delivery_wait(started.elapsed());
            }
        }
        self.reserved_bytes -= message.weighted_bytes;
        if message.status == MsgStatus::Delivering {
            self.inflight_bytes -= message.payload_bytes;
        }
        let Some(socket_state) = self.sockets.get_mut(&message.socket_id) else {
            return;
        };
        socket_state.messages.remove(&token);
        if message.status == MsgStatus::Delivering {
            let still_delivering = socket_state.messages.iter().any(|token| {
                matches!(
                    self.messages.get(token),
                    Some(MsgEntry {
                        status: MsgStatus::Delivering,
                        ..
                    })
                )
            });
            if !still_delivering {
                if let Some(key) = socket_state.blocked_key.take() {
                    self.blocked.remove(&key);
                    self.blocked_sources -= 1;
                }
            }
        }
    }

    fn expire_reservations(&mut self) {
        let now = Instant::now();
        let expired: Vec<u64> = self
            .connections
            .iter()
            .filter_map(|(token, status)| match status {
                ConnStatus::Reservation { expires_at } if *expires_at <= now => Some(*token),
                _live => None,
            })
            .collect();
        for token in expired {
            self.release_connection(token);
        }
    }

    fn shed_if_needed(&mut self) {
        let memory = (self.memory_probe)();
        let recovery = self.recovery_threshold();

        if self.watermark == 0 {
            self.pressure = None;
            return;
        }
        if self.pressure.is_some() && memory <= recovery {
            self.pressure = None;
            return;
        }
        if memory >= self.watermark as u64 || self.pressure.is_some() {
            let batch = self.pressure_batch(memory, recovery);
            let victims = self.shed_candidates(batch);
            self.pressure = Some(PressureState {
                memory,
                victims,
                batch,
            });
            if !self.active.is_empty() || !self.blocked.is_empty() {
                self.pressure_recheck = true;
            }
        }
    }

    fn take_pressure_recheck(&mut self) -> Option<Instant> {
        if self.pressure_recheck {
            Some(Instant::now() + PRESSURE_RECHECK)
        } else {
            None
        }
    }

    fn recovery_threshold(&self) -> u64 {
        (self.watermark as u64).saturating_sub(protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES as u64)
    }

    fn pressure_batch(&self, memory: u64, recovery: u64) -> usize {
        let maximum_message = protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES as u64;
        match &self.pressure {
            None => {
                let excess = memory.saturating_sub(self.watermark as u64);
                let batch = excess.div_ceil(maximum_message).max(1) as usize;
                batch.min(INITIAL_MAX_SHED_BATCH)
            }
            Some(previous) => {
                let relief = previous.memory.saturating_sub(memory);
                if relief > 0 && previous.victims > 0 {
                    let bytes_per_victim = (relief / previous.victims as u64).max(1);
                    let needed = memory.saturating_sub(recovery).div_ceil(bytes_per_victim);
                    (needed.max(1) as usize).min(MAX_SHED_BATCH)
                } else {
                    (previous.batch * 2).clamp(1, MAX_SHED_BATCH)
                }
            }
        }
    }

    fn shed_candidates(&mut self, batch: usize) -> usize {
        let mut victims = 0;
        for _ in 0..batch {
            let socket_id = if let Some((&key, &socket_id)) = self.blocked.iter().next() {
                // Oldest blocked source holds the most reserved memory.
                self.blocked.remove(&key);
                socket_id
            } else if let Some((&key, &socket_id)) = self.active.iter().next_back() {
                // Newest active socket; protect long-lived sessions.
                self.active.remove(&key);
                socket_id
            } else {
                break;
            };
            if let Some(socket_state) = self.sockets.get_mut(&socket_id) {
                if let Some(key) = socket_state.active_key.take() {
                    self.active.remove(&key);
                }
                if let Some(key) = socket_state.blocked_key.take() {
                    self.blocked.remove(&key);
                    self.blocked_sources -= 1;
                }
                socket_state.shedding = true;
                let _ = socket_state.signal.send(SocketMsg::MemoryPressure);
                Metrics::inc(&self.metrics.memory_pressure_disconnects);
                victims += 1;
            }
        }
        victims
    }

    fn next_key(&mut self) -> (Instant, u64) {
        (Instant::now(), next_id())
    }

    fn gauges(&self) -> CapacityGauges {
        CapacityGauges {
            active_websockets: self.active_websockets as u64,
            ingress_reserved_bytes: self.reserved_bytes as u64,
            inflight_delivery_bytes: self.inflight_bytes as u64,
            backpressured_sources: self.blocked_sources as u64,
        }
    }
}
