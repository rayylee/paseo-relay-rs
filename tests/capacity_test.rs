mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::*;

#[tokio::test]
async fn rejects_connections_above_max_connections_with_503() {
    let relay = start_relay_with(
        paseo_relay::config::Config {
            max_connections: 2,
            ..test_config()
        },
        None,
    )
    .await;

    let _first = client(&relay, "srv-cap", "conn_a").await;
    let _second = client(&relay, "srv-cap", "conn_b").await;
    assert_eq!(
        connect_err(&relay, "serverId=srv-cap&role=client&v=2").await,
        503
    );
}

#[tokio::test]
async fn releases_capacity_when_connection_closes() {
    let relay = start_relay_with(
        paseo_relay::config::Config {
            max_connections: 1,
            ..test_config()
        },
        None,
    )
    .await;

    let first = client(&relay, "srv-release", "conn_a").await;
    assert_eq!(
        connect_err(&relay, "serverId=srv-release&role=client&v=2").await,
        503
    );
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _second = client(&relay, "srv-release", "conn_b").await;
}

#[tokio::test]
async fn backpressure_pauses_source_until_delivery_completes() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-bp", "conn_a").await;
    let mut client_socket = client(&relay, "srv-bp", "conn_a").await;

    // Rapid-fire frames; ordering must hold under the one-inflight-delivery
    // backpressure regime.
    for index in 0..50 {
        send_text(&mut client_socket, &format!("frame-{index}")).await;
    }
    for index in 0..50 {
        assert_eq!(recv_text(&mut data).await, format!("frame-{index}"));
    }
}

#[tokio::test]
async fn reverse_traffic_flows_while_forward_delivery_waits() {
    let relay = start_relay().await;
    // No data socket yet: the client's frame parks waiting for attach.
    let mut client_socket = client(&relay, "srv-bidi", "conn_a").await;
    send_text(&mut client_socket, "parked").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Attaching the data socket releases the waiter; both directions flow.
    let mut data = data_socket(&relay, "srv-bidi", "conn_a").await;
    assert_eq!(recv_text(&mut data).await, "parked");
    send_text(&mut data, "downstream").await;
    assert_eq!(recv_text(&mut client_socket).await, "downstream");
}

#[tokio::test]
async fn client_frame_times_out_with_1013_when_no_data_socket_attaches() {
    let relay = start_relay_with(
        paseo_relay::config::Config {
            data_attach_timeout_ms: 1_000,
            ..test_config()
        },
        None,
    )
    .await;

    let mut client_socket = client(&relay, "srv-timeout", "conn_a").await;
    send_text(&mut client_socket, "doomed").await;
    let (code, reason) = recv_close(&mut client_socket).await;
    assert_eq!(code, 1013);
    assert_eq!(reason, "Data route unavailable");
}

#[tokio::test]
async fn memory_pressure_sheds_sockets_with_1013() {
    let simulated_memory = Arc::new(AtomicU64::new(0));
    let probe_memory = simulated_memory.clone();
    let relay = start_relay_with(
        paseo_relay::config::Config {
            memory_watermark_bytes: 512 * 1024 * 1024,
            ..test_config()
        },
        Some(Arc::new(move || probe_memory.load(Ordering::SeqCst))),
    )
    .await;

    let mut victim = client(&relay, "srv-shed", "conn_a").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Push simulated RSS above the watermark; the next periodic check sheds.
    simulated_memory.store(600 * 1024 * 1024, Ordering::SeqCst);
    let (code, reason) = recv_close(&mut victim).await;
    assert_eq!(code, 1013);
    assert_eq!(reason, "Relay memory pressure");

    // While under pressure, new upgrades are rejected.
    assert_eq!(
        connect_err(&relay, "serverId=srv-shed2&role=client&v=2").await,
        503
    );

    // Recovery: memory drops below the recovery threshold, admissions resume.
    simulated_memory.store(100 * 1024 * 1024, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let _admitted = client(&relay, "srv-shed3", "conn_a").await;
}

#[tokio::test]
async fn ingress_budget_exhaustion_closes_source_with_1013() {
    // Weight 16, budget 512 MiB → a single 20 MiB frame reserves 320 MiB;
    // a second in-flight frame would exceed the budget.
    let relay = start_relay_with(
        paseo_relay::config::Config {
            ingress_weight: 16,
            ..test_config()
        },
        None,
    )
    .await;

    // No data socket: frames park in delivery, holding their reservations.
    let mut client_socket = client(&relay, "srv-budget", "conn_a").await;
    let big = vec![b'x'; 20 * 1024 * 1024];
    send_binary(&mut client_socket, big.clone()).await;

    let mut second = client(&relay, "srv-budget", "conn_b").await;
    send_binary(&mut second, big).await;
    let (code, reason) = recv_close(&mut second).await;
    assert_eq!(code, 1013);
    assert_eq!(reason, "Relay ingress capacity");
}
