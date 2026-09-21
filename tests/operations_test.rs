mod common;

use common::*;

#[tokio::test]
async fn health_always_returns_200() {
    let relay = start_relay().await;
    let (status, body) = http_get(&relay, "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ok"}"#);

    relay.drain.begin();
    let (status, _body) = http_get(&relay, "/health").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn ready_reflects_drain_state() {
    let relay = start_relay().await;
    let (status, body) = http_get(&relay, "/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"status":"ready"}"#);

    relay.drain.begin();
    let (status, body) = http_get(&relay, "/ready").await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"status":"unready"}"#);

    relay.drain.cancel();
    let (status, _body) = http_get(&relay, "/ready").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn drain_rejects_new_sessions_with_503() {
    let relay = start_relay().await;
    relay.drain.begin();
    assert_eq!(connect_err(&relay, "serverId=x&role=client&v=2").await, 503);
}

#[tokio::test]
async fn metrics_exposes_expected_series() {
    let relay = start_relay().await;

    // Produce one forwarded frame so counters move.
    let mut data = data_socket(&relay, "srv-metrics", "conn_a").await;
    let mut client_socket = client(&relay, "srv-metrics", "conn_a").await;
    send_text(&mut client_socket, "count-me").await;
    assert_eq!(recv_text(&mut data).await, "count-me");

    let (status, body) = http_get(&relay, "/metrics").await;
    assert_eq!(status, 200);
    for series in [
        "paseo_relay_ready 1",
        "paseo_relay_draining 0",
        "paseo_relay_active_websockets 2",
        "paseo_relay_active_sessions 1",
        "paseo_relay_connection_rejections_total",
        "paseo_relay_frames_forwarded_total 1",
        "paseo_relay_bytes_forwarded_total 8",
        "paseo_relay_ingress_reserved_bytes",
        "paseo_relay_inflight_delivery_bytes",
        "paseo_relay_backpressured_sources",
        "paseo_relay_slow_consumer_disconnects_total",
        "paseo_relay_delivery_timeouts_total",
        "paseo_relay_memory_pressure_disconnects_total",
        "paseo_relay_max_frame_bytes 8",
        "paseo_relay_delivery_wait_seconds_bucket",
        "paseo_relay_frame_size_bytes_bucket",
        "paseo_relay_handshake_accepted_total{routing_version=\"v2\",type=\"hello\"}",
        "paseo_relay_handshake_rejected_total{routing_version=\"v2\",type=\"e2ee_hello\"}",
        "paseo_relay_process_memory_bytes",
    ] {
        assert!(body.contains(series), "missing series: {series}\n{body}");
    }
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let relay = start_relay().await;
    let (status, _body) = http_get(&relay, "/nope").await;
    assert_eq!(status, 404);
}
