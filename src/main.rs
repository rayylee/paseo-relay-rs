use std::sync::Arc;

use paseo_relay::config::Config;
use paseo_relay::drain::Drain;
use paseo_relay::metrics::Metrics;
use paseo_relay::node::{self, NodeOptions};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = match Config::load_from_env() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("configuration error: {message}");
            std::process::exit(1);
        }
    };

    let drain = Arc::new(Drain::new(config.drain));
    let metrics = Arc::new(Metrics::default());

    let node = match node::start(NodeOptions {
        config,
        drain: drain.clone(),
        metrics,
        memory_probe: None,
    })
    .await
    {
        Ok(node) => node,
        Err(error) => {
            eprintln!("failed to bind listener: {error}");
            std::process::exit(1);
        }
    };

    tracing::info!("paseo-relay listening on {}", node.local_addr);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            drain.begin();
            node.cancel.cancel();
        }
        _ = node.done => {}
    }
}
