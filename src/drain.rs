use std::sync::atomic::{AtomicBool, Ordering};

/// Process-local admission state for graceful relay maintenance. Beginning a
/// drain makes readiness fail immediately; existing sessions are left to the
/// routing layer.
#[derive(Debug)]
pub struct Drain {
    draining: AtomicBool,
}

impl Drain {
    pub fn new(initially_draining: bool) -> Drain {
        Drain {
            draining: AtomicBool::new(initially_draining),
        }
    }

    pub fn begin(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn cancel(&self) {
        self.draining.store(false, Ordering::SeqCst);
    }

    pub fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
}
