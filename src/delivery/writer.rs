use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::Message;
use tokio::sync::{mpsc, oneshot};

use crate::metrics::Metrics;
use crate::next_id;
use crate::socket::SocketMsg;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    Timeout,
    DestinationClosed,
    SourceClosed,
    InvalidReservation,
}

enum Msg {
    Reserve {
        deadline: Instant,
        source_done: oneshot::Receiver<()>,
        reply: oneshot::Sender<Result<u64, WriteError>>,
    },
    Write {
        token: u64,
        message: Message,
        reply: oneshot::Sender<Result<(), WriteError>>,
    },
    Written {
        reference: u64,
    },
    Control {
        payload: String,
        reply: oneshot::Sender<Result<(), WriteError>>,
    },
    Close {
        code: u16,
        reason: &'static str,
    },
}

/// Handle to the per-destination write serializer. One payload is in flight at
/// a time; a write completes only when the destination's sink task acknowledges
/// the write barrier.
#[derive(Clone)]
pub struct WriterHandle {
    sender: mpsc::UnboundedSender<Msg>,
}

/// Dropped when the destination socket task exits; wakes the writer so it can
/// reject queued work (the `Process.monitor(destination)` analog).
pub struct WriterDestinationGuard {
    _stop: oneshot::Sender<()>,
}

impl WriterHandle {
    /// Reserve the active write slot. `source_done` is dropped when the source
    /// socket exits, letting the writer release a stale reservation.
    pub async fn reserve(
        &self,
        deadline: Instant,
    ) -> Result<(u64, oneshot::Sender<()>), WriteError> {
        if Instant::now() >= deadline {
            return Err(WriteError::Timeout);
        }
        let (done_tx, done_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Msg::Reserve {
                deadline,
                source_done: done_rx,
                reply: reply_tx,
            })
            .map_err(|_| WriteError::DestinationClosed)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(Ok(Ok(token))) => Ok((token, done_tx)),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_closed)) => Err(WriteError::DestinationClosed),
            Err(_timeout) => Err(WriteError::Timeout),
        }
    }

    pub async fn write(
        &self,
        token: u64,
        message: Message,
        deadline: Instant,
    ) -> Result<(), WriteError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Msg::Write {
                token,
                message,
                reply: reply_tx,
            })
            .map_err(|_| WriteError::DestinationClosed)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_closed)) => Err(WriteError::DestinationClosed),
            Err(_timeout) => Err(WriteError::Timeout),
        }
    }

    pub fn acknowledge(&self, reference: u64) {
        let _ = self.sender.send(Msg::Written { reference });
    }

    pub fn control(
        &self,
        payload: String,
    ) -> Result<oneshot::Receiver<Result<(), WriteError>>, WriteError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Msg::Control {
                payload,
                reply: reply_tx,
            })
            .map_err(|_| WriteError::DestinationClosed)?;
        Ok(reply_rx)
    }

    pub fn close(&self, code: u16, reason: &'static str) {
        let _ = self.sender.send(Msg::Close { code, reason });
    }
}

struct ActiveWrite {
    token: u64,
    reservation_deadline: Instant,
    source_done: Option<oneshot::Receiver<()>>,
    write_reply: Option<oneshot::Sender<Result<(), WriteError>>>,
    write_reference: Option<u64>,
}

enum Queued {
    Payload {
        deadline: Instant,
        source_done: oneshot::Receiver<()>,
        reply: oneshot::Sender<Result<u64, WriteError>>,
    },
    Control {
        payload: String,
        deadline: Instant,
    },
}

struct WriterState {
    destination: mpsc::UnboundedSender<SocketMsg>,
    self_handle: WriterHandle,
    delivery_timeout: Duration,
    control_queue_bytes: usize,
    active: Option<ActiveWrite>,
    queued: VecDeque<Queued>,
    queued_control_bytes: usize,
    metrics: Arc<Metrics>,
}

pub fn spawn(
    destination: mpsc::UnboundedSender<SocketMsg>,
    delivery_timeout: Duration,
    control_queue_bytes: usize,
    metrics: Arc<Metrics>,
) -> (WriterHandle, WriterDestinationGuard) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = oneshot::channel();
    let state = WriterState {
        destination,
        self_handle: WriterHandle {
            sender: sender.clone(),
        },
        delivery_timeout,
        control_queue_bytes,
        active: None,
        queued: VecDeque::new(),
        queued_control_bytes: 0,
        metrics,
    };
    tokio::spawn(run(state, receiver, stop_rx));
    (
        WriterHandle { sender },
        WriterDestinationGuard { _stop: stop_tx },
    )
}

async fn run(
    mut state: WriterState,
    mut receiver: mpsc::UnboundedReceiver<Msg>,
    mut destination_gone: oneshot::Receiver<()>,
) {
    loop {
        let reservation_deadline = state
            .active
            .as_ref()
            .map(|active| active.reservation_deadline);
        let reservation_timeout = async {
            match reservation_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                None => std::future::pending().await,
            }
        };
        // Temporarily take the source-liveness receiver so the select! arms
        // can still borrow `state` mutably; restored after the select if the
        // same reservation is still active.
        let taken_token = state.active.as_ref().map(|active| active.token);
        let mut taken_source_done = state
            .active
            .as_mut()
            .and_then(|active| active.source_done.take());
        let source_gone = async {
            match &mut taken_source_done {
                Some(done) => {
                    let _ = done.await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::pin!(source_gone);

        enum Step {
            Message(Option<Msg>),
            DestinationGone,
            ReservationTimeout,
            SourceGone,
        }

        let step = tokio::select! {
            _ = &mut destination_gone => Step::DestinationGone,
            message = receiver.recv() => Step::Message(message),
            _ = reservation_timeout => Step::ReservationTimeout,
            _ = &mut source_gone => Step::SourceGone,
        };

        // Restore the receiver unless the source is confirmed gone or the
        // reservation changed under us.
        if !matches!(step, Step::SourceGone) {
            if let Some(active) = state.active.as_mut() {
                if Some(active.token) == taken_token {
                    active.source_done = taken_source_done.take();
                }
            }
        }

        match step {
            Step::DestinationGone => {
                state.reject_all(WriteError::DestinationClosed);
                return;
            }
            Step::Message(None) => {
                state.reject_all(WriteError::DestinationClosed);
                return;
            }
            Step::Message(Some(message)) => {
                if !state.handle(message) {
                    return;
                }
            }
            Step::ReservationTimeout => {
                // Active slot exceeded the delivery deadline: shed the slow consumer.
                tracing::warn!("closing slow consumer: delivery deadline exceeded");
                Metrics::inc(&state.metrics.delivery_timeouts);
                Metrics::inc(&state.metrics.slow_consumer_disconnects);
                let _ = state.destination.send(SocketMsg::Close {
                    code: 1013,
                    reason: "Slow consumer",
                });
                state.reject_all(WriteError::Timeout);
                return;
            }
            Step::SourceGone => {
                let active = state.active.take().unwrap();
                if active.write_reference.is_some() {
                    // Frame already sent toward the destination; its state is
                    // ambiguous, so fail it closed.
                    let _ = state.destination.send(SocketMsg::Close {
                        code: 1013,
                        reason: "Delivery unavailable",
                    });
                    state.reject_all(WriteError::SourceClosed);
                    return;
                }
                if let Some(reply) = active.write_reply {
                    let _ = reply.send(Err(WriteError::SourceClosed));
                }
                if !state.grant_next() {
                    return;
                }
            }
        }
    }
}

impl WriterState {
    /// Returns false when the writer should stop.
    fn handle(&mut self, message: Msg) -> bool {
        match message {
            Msg::Reserve {
                deadline,
                source_done,
                reply,
            } => {
                if self.active.is_none() {
                    if Instant::now() >= deadline {
                        let _ = reply.send(Err(WriteError::Timeout));
                    } else {
                        self.grant(deadline, source_done, reply);
                    }
                } else {
                    self.queued.push_back(Queued::Payload {
                        deadline,
                        source_done,
                        reply,
                    });
                }
                true
            }
            Msg::Write {
                token,
                message,
                reply,
            } => {
                match &mut self.active {
                    Some(active) if active.token == token => {
                        let reference = next_id();
                        Metrics::inc(&self.metrics.frames_forwarded);
                        Metrics::add(
                            &self.metrics.bytes_forwarded,
                            crate::protocol::payload_len(&message) as u64,
                        );
                        let frame_ok = self.destination.send(SocketMsg::Frame { message }).is_ok();
                        let barrier_ok = frame_ok
                            && self
                                .destination
                                .send(SocketMsg::Barrier {
                                    reference,
                                    writer: self.self_handle.clone(),
                                })
                                .is_ok();
                        if barrier_ok {
                            active.write_reply = Some(reply);
                            active.write_reference = Some(reference);
                        } else {
                            let _ = reply.send(Err(WriteError::DestinationClosed));
                            self.reject_all(WriteError::DestinationClosed);
                            return false;
                        }
                    }
                    _stale => {
                        let _ = reply.send(Err(WriteError::InvalidReservation));
                    }
                }
                true
            }
            Msg::Written { reference } => {
                let matches = self
                    .active
                    .as_ref()
                    .and_then(|active| active.write_reference)
                    == Some(reference);
                if matches {
                    let active = self.active.take().unwrap();
                    if let Some(reply) = active.write_reply {
                        let _ = reply.send(Ok(()));
                    }
                    return self.grant_next();
                }
                true
            }
            Msg::Control { payload, reply } => {
                let deadline = Instant::now() + self.delivery_timeout;
                if self.active.is_none() {
                    self.start_control(payload, deadline);
                    let _ = reply.send(Ok(()));
                    true
                } else {
                    let bytes = payload.len();
                    if self.queued_control_bytes + bytes <= self.control_queue_bytes {
                        self.queued_control_bytes += bytes;
                        let _ = reply.send(Ok(()));
                        self.queued.push_back(Queued::Control { payload, deadline });
                        true
                    } else {
                        tracing::warn!("closing slow consumer: control queue full");
                        Metrics::inc(&self.metrics.slow_consumer_disconnects);
                        let _ = self.destination.send(SocketMsg::Close {
                            code: 1013,
                            reason: "Slow consumer",
                        });
                        let _ = reply.send(Err(WriteError::Timeout));
                        self.reject_all(WriteError::Timeout);
                        false
                    }
                }
            }
            Msg::Close { code, reason } => {
                let _ = self.destination.send(SocketMsg::Close { code, reason });
                self.reject_all(WriteError::DestinationClosed);
                false
            }
        }
    }

    fn grant(
        &mut self,
        deadline: Instant,
        source_done: oneshot::Receiver<()>,
        reply: oneshot::Sender<Result<u64, WriteError>>,
    ) {
        let token = next_id();
        self.active = Some(ActiveWrite {
            token,
            reservation_deadline: deadline,
            source_done: Some(source_done),
            write_reply: None,
            write_reference: None,
        });
        if reply.send(Ok(token)).is_err() {
            // Source gave up before the grant landed; free the slot.
            self.active = None;
        }
    }

    /// Returns false when the writer should stop.
    fn grant_next(&mut self) -> bool {
        while let Some(entry) = self.queued.pop_front() {
            match entry {
                Queued::Payload {
                    deadline,
                    source_done,
                    reply,
                } => {
                    if Instant::now() >= deadline {
                        let _ = reply.send(Err(WriteError::Timeout));
                        continue;
                    }
                    self.grant(deadline, source_done, reply);
                    if self.active.is_some() {
                        return true;
                    }
                }
                Queued::Control { payload, deadline } => {
                    self.queued_control_bytes -= payload.len();
                    if Instant::now() >= deadline {
                        tracing::warn!("closing slow consumer: queued control deadline exceeded");
                        Metrics::inc(&self.metrics.delivery_timeouts);
                        Metrics::inc(&self.metrics.slow_consumer_disconnects);
                        let _ = self.destination.send(SocketMsg::Close {
                            code: 1013,
                            reason: "Slow consumer",
                        });
                        self.reject_all(WriteError::Timeout);
                        return false;
                    }
                    self.start_control(payload, deadline);
                    return true;
                }
            }
        }
        true
    }

    fn start_control(&mut self, payload: String, deadline: Instant) {
        let reference = next_id();
        Metrics::inc(&self.metrics.frames_forwarded);
        Metrics::add(&self.metrics.bytes_forwarded, payload.len() as u64);
        let _ = self.destination.send(SocketMsg::Frame {
            message: Message::Text(payload.into()),
        });
        let _ = self.destination.send(SocketMsg::Barrier {
            reference,
            writer: self.self_handle.clone(),
        });
        self.active = Some(ActiveWrite {
            token: reference,
            reservation_deadline: deadline,
            source_done: None,
            write_reply: None,
            write_reference: Some(reference),
        });
    }

    fn reject_all(&mut self, error: WriteError) {
        if let Some(active) = self.active.take() {
            if let Some(reply) = active.write_reply {
                let _ = reply.send(Err(error));
            }
        }
        for entry in self.queued.drain(..) {
            match entry {
                Queued::Payload { reply, .. } => {
                    let _ = reply.send(Err(error));
                }
                Queued::Control { .. } => {}
            }
        }
        self.queued_control_bytes = 0;
    }
}
