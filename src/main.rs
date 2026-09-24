use std::sync::Arc;

use paseo_relay::config::Config;
use paseo_relay::drain::Drain;
use paseo_relay::logging;
use paseo_relay::metrics::Metrics;
use paseo_relay::node::{self, NodeOptions};

#[tokio::main]
async fn main() {
    let config = match Config::load_from_env() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("configuration error: {message}");
            std::process::exit(1);
        }
    };

    // `PASEO_RELAY_DEBUG` also raises the fallback log level so the debug
    // pairing console run is easier to follow. `RUST_LOG` still wins.
    logging::init(if config.debug { "debug" } else { "info" });

    tracing::info!(
        host = %config.host,
        port = config.port,
        max_connections = config.max_connections,
        memory_watermark_bytes = config.memory_watermark_bytes,
        debug = config.debug,
        "starting paseo-relay"
    );

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
            tracing::error!("failed to bind listener: {error}");
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
