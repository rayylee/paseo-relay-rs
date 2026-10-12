pub mod capacity;
pub mod config;
pub mod connection;
pub mod delivery;
pub mod drain;
pub mod handshake;
pub mod logging;
pub mod metrics;
pub mod node;
pub mod operations;
pub mod ownership;
pub mod protocol;
pub mod socket;
pub mod web;

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Process-wide unique identifier, used for socket ids, tokens, and
/// write-barrier references.
pub fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}
