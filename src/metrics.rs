use std::sync::atomic::{AtomicU64, Ordering};

use crate::handshake::HandshakeType;
use crate::protocol;

const DELIVERY_BUCKETS: [(u64, &str); 5] = [
    (1_000, "0.001"),
    (10_000, "0.01"),
    (100_000, "0.1"),
    (1_000_000, "1"),
    (10_000_000, "10"),
];

const FRAME_BUCKET_LIMITS: [usize; 5] = [
    1024,
    64 * 1024,
    1024 * 1024,
    8 * 1024 * 1024,
    protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES,
];

#[derive(Debug, Default)]
pub struct Metrics {
    pub reroute_responses: AtomicU64,
    pub connection_rejections: AtomicU64,
    pub frames_forwarded: AtomicU64,
    pub bytes_forwarded: AtomicU64,
    pub slow_consumer_disconnects: AtomicU64,
    pub delivery_timeouts: AtomicU64,
    pub memory_pressure_disconnects: AtomicU64,
    pub max_frame_bytes: AtomicU64,

    handshake_accepted: [AtomicU64; 2],
    handshake_rejected: [AtomicU64; 2],

    delivery_wait_buckets: [AtomicU64; 5],
    delivery_wait_count: AtomicU64,
    delivery_wait_microseconds: AtomicU64,

    frame_size_buckets: [AtomicU64; 5],
    frame_size_count: AtomicU64,
    frame_size_bytes_sum: AtomicU64,
}

/// Point-in-time gauges owned by the capacity actor, folded into `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CapacityGauges {
    pub active_websockets: u64,
    pub ingress_reserved_bytes: u64,
    pub inflight_delivery_bytes: u64,
    pub backpressured_sources: u64,
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn observe_frame(&self, byte_count: usize) {
        Metrics::inc(&self.frame_size_count);
        Metrics::add(&self.frame_size_bytes_sum, byte_count as u64);
        for (index, limit) in FRAME_BUCKET_LIMITS.iter().enumerate() {
            if byte_count <= *limit {
                Metrics::inc(&self.frame_size_buckets[index]);
            }
        }
        self.max_frame_bytes
            .fetch_max(byte_count as u64, Ordering::Relaxed);
    }

    pub fn observe_delivery_wait(&self, wait: std::time::Duration) {
        let microseconds = wait.as_micros() as u64;
        Metrics::inc(&self.delivery_wait_count);
        Metrics::add(&self.delivery_wait_microseconds, microseconds);
        for (index, (limit, _label)) in DELIVERY_BUCKETS.iter().enumerate() {
            if microseconds <= *limit {
                Metrics::inc(&self.delivery_wait_buckets[index]);
            }
        }
    }

    pub fn observe_handshake(&self, accepted: bool, handshake_type: HandshakeType) {
        let index = match handshake_type {
            HandshakeType::Hello => 0,
            HandshakeType::E2eeHello => 1,
        };
        let family = if accepted {
            &self.handshake_accepted
        } else {
            &self.handshake_rejected
        };
        Metrics::inc(&family[index]);
    }

    pub fn render(
        &self,
        ready: bool,
        draining: bool,
        capacity: Option<CapacityGauges>,
        active_sessions: u64,
    ) -> String {
        let mut output = String::with_capacity(8 * 1024);
        let gauge = |output: &mut String, name: &str, help: &str, value: u64| {
            render_metric(output, name, "gauge", help, value);
        };
        let counter = |output: &mut String, name: &str, help: &str, value: &AtomicU64| {
            render_metric(output, name, "counter", help, value.load(Ordering::Relaxed));
        };

        gauge(
            &mut output,
            "paseo_relay_ready",
            "Whether this node admits new relay work.",
            ready as u64,
        );
        gauge(
            &mut output,
            "paseo_relay_draining",
            "Whether this node is draining.",
            draining as u64,
        );
        if let Some(capacity) = capacity {
            gauge(
                &mut output,
                "paseo_relay_active_websockets",
                "Open WebSocket connections on this node.",
                capacity.active_websockets,
            );
        }
        gauge(
            &mut output,
            "paseo_relay_active_sessions",
            "Relay sessions owned by this node.",
            active_sessions,
        );
        counter(
            &mut output,
            "paseo_relay_reroute_responses_total",
            "WebSocket upgrades rerouted to another owner.",
            &self.reroute_responses,
        );
        counter(
            &mut output,
            "paseo_relay_connection_rejections_total",
            "WebSocket upgrades rejected at configured capacity or during memory pressure.",
            &self.connection_rejections,
        );
        counter(
            &mut output,
            "paseo_relay_frames_forwarded_total",
            "WebSocket frames forwarded by this node.",
            &self.frames_forwarded,
        );
        counter(
            &mut output,
            "paseo_relay_bytes_forwarded_total",
            "WebSocket payload bytes forwarded by this node.",
            &self.bytes_forwarded,
        );
        if let Some(capacity) = capacity {
            gauge(
                &mut output,
                "paseo_relay_ingress_reserved_bytes",
                "Weighted ingress bytes admitted on this node.",
                capacity.ingress_reserved_bytes,
            );
            gauge(
                &mut output,
                "paseo_relay_inflight_delivery_bytes",
                "Payload bytes currently held by synchronous downstream delivery.",
                capacity.inflight_delivery_bytes,
            );
            gauge(
                &mut output,
                "paseo_relay_backpressured_sources",
                "Source WebSockets currently waiting for downstream delivery.",
                capacity.backpressured_sources,
            );
        }
        counter(
            &mut output,
            "paseo_relay_slow_consumer_disconnects_total",
            "Destinations disconnected after exceeding a delivery deadline.",
            &self.slow_consumer_disconnects,
        );
        counter(
            &mut output,
            "paseo_relay_delivery_timeouts_total",
            "Synchronous downstream deliveries that exceeded their deadline.",
            &self.delivery_timeouts,
        );
        counter(
            &mut output,
            "paseo_relay_memory_pressure_disconnects_total",
            "WebSockets closed by node memory-pressure recovery.",
            &self.memory_pressure_disconnects,
        );
        gauge(
            &mut output,
            "paseo_relay_max_frame_bytes",
            "Largest WebSocket frame payload observed since node start.",
            self.max_frame_bytes.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "paseo_relay_process_memory_bytes",
            "Resident set size of the relay process.",
            process_rss_bytes(),
        );

        self.render_handshake(&mut output);
        self.render_delivery_histogram(&mut output);
        self.render_frame_histogram(&mut output);
        output
    }

    fn render_handshake(&self, output: &mut String) {
        for (outcome, family) in [
            ("accepted", &self.handshake_accepted),
            ("rejected", &self.handshake_rejected),
        ] {
            let name = format!("paseo_relay_handshake_{outcome}_total");
            output.push_str(&format!(
                "# HELP {name} Client E2EE handshake frames {outcome} by the handshake input validator.\n# TYPE {name} counter\n"
            ));
            for (index, handshake_type) in ["hello", "e2ee_hello"].iter().enumerate() {
                output.push_str(&format!(
                    "{name}{{routing_version=\"v2\",type=\"{handshake_type}\"}} {}\n",
                    family[index].load(Ordering::Relaxed)
                ));
            }
        }
    }

    fn render_delivery_histogram(&self, output: &mut String) {
        let name = "paseo_relay_delivery_wait_seconds";
        output.push_str(&format!(
            "# HELP {name} Time a source waits for synchronous downstream delivery.\n# TYPE {name} histogram\n"
        ));
        for (index, (_limit, label)) in DELIVERY_BUCKETS.iter().enumerate() {
            output.push_str(&format!(
                "{name}_bucket{{le=\"{label}\"}} {}\n",
                self.delivery_wait_buckets[index].load(Ordering::Relaxed)
            ));
        }
        let count = self.delivery_wait_count.load(Ordering::Relaxed);
        let sum_seconds =
            self.delivery_wait_microseconds.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        output.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {count}\n"));
        output.push_str(&format!("{name}_sum {sum_seconds}\n"));
        output.push_str(&format!("{name}_count {count}\n"));
    }

    fn render_frame_histogram(&self, output: &mut String) {
        let name = "paseo_relay_frame_size_bytes";
        output.push_str(&format!(
            "# HELP {name} WebSocket payload-size distribution.\n# TYPE {name} histogram\n"
        ));
        for (index, limit) in FRAME_BUCKET_LIMITS.iter().enumerate() {
            output.push_str(&format!(
                "{name}_bucket{{le=\"{limit}\"}} {}\n",
                self.frame_size_buckets[index].load(Ordering::Relaxed)
            ));
        }
        let count = self.frame_size_count.load(Ordering::Relaxed);
        output.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {count}\n"));
        output.push_str(&format!(
            "{name}_sum {}\n",
            self.frame_size_bytes_sum.load(Ordering::Relaxed)
        ));
        output.push_str(&format!("{name}_count {count}\n"));
    }
}

fn render_metric(output: &mut String, name: &str, kind: &str, help: &str, value: u64) {
    output.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
    ));
}

/// Current resident set size in bytes, from /proc/self/status (Linux). Returns
/// 0 where unavailable so metrics rendering never fails.
pub fn process_rss_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kilobytes: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kilobytes * 1024;
        }
    }
    0
}
