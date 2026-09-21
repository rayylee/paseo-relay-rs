pub mod owner;

use std::sync::Arc;

use dashmap::DashMap;

use owner::OwnerHandle;

pub enum RouteResult {
    Local {
        owner: OwnerHandle,
        reservation: u64,
    },
    Unavailable,
}

/// Local session registry: one owner task per serverId (the single-node
/// replacement for the Syn-based distributed registry).
pub struct Ownership {
    owners: Arc<DashMap<String, OwnerHandle>>,
}

impl Default for Ownership {
    fn default() -> Self {
        Self::new()
    }
}

impl Ownership {
    pub fn new() -> Ownership {
        Ownership {
            owners: Arc::new(DashMap::new()),
        }
    }

    pub fn active_sessions(&self) -> usize {
        self.owners.len()
    }

    pub async fn route(&self, server_id: &str, draining: bool) -> RouteResult {
        // A looked-up owner may have just gone idle and exited; retry once
        // against a freshly spawned owner before giving up.
        for _attempt in 0..2 {
            let existing = self
                .owners
                .get(server_id)
                .map(|entry| entry.value().clone());
            let (owner, created) = match existing {
                Some(owner) => (owner, false),
                None => {
                    if draining {
                        return RouteResult::Unavailable;
                    }
                    let owner = self
                        .owners
                        .entry(server_id.to_string())
                        .or_insert_with(|| owner::spawn(server_id.to_string(), self.owners.clone()))
                        .value()
                        .clone();
                    (owner, true)
                }
            };
            match owner.reserve().await {
                Ok(reservation) => return RouteResult::Local { owner, reservation },
                Err(_closed) if !created => {
                    // Stale registration; retry the loop to spawn a new owner.
                    continue;
                }
                Err(_closed) => return RouteResult::Unavailable,
            }
        }
        RouteResult::Unavailable
    }
}
