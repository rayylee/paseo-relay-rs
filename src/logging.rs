use tracing_subscriber::EnvFilter;

/// Initialise the process-wide tracing subscriber.
///
/// Verbosity follows the `RUST_LOG` environment variable when present, and
/// falls back to `fallback` otherwise. The subscriber is installed at most once
/// per process, so this is meant to be called from `main` (or a test harness).
pub fn init(fallback: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
