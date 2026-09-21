mod common;

use common::*;
use futures_util::StreamExt;

#[tokio::test]
async fn control_socket_receives_sync_on_connect() {
    let relay = start_relay().await;
    let mut control = control(&relay, "srv-sync").await;
    let sync = recv_json(&mut control).await;
    assert_eq!(sync["type"], "sync");
    assert_eq!(sync["connectionIds"], serde_json::json!([]));
}

#[tokio::test]
async fn control_receives_connected_and_disconnected_notifications() {
    let relay = start_relay().await;
    let mut control = control(&relay, "srv-notify").await;
    assert_eq!(recv_json(&mut control).await["type"], "sync");

    let client_socket = client(&relay, "srv-notify", "conn_a").await;
    let connected = recv_json(&mut control).await;
    assert_eq!(connected["type"], "connected");
    assert_eq!(connected["connectionId"], "conn_a");

    drop(client_socket);
    let disconnected = recv_json(&mut control).await;
    assert_eq!(disconnected["type"], "disconnected");
    assert_eq!(disconnected["connectionId"], "conn_a");
}

#[tokio::test]
async fn sync_lists_connected_clients() {
    let relay = start_relay().await;
    let _client = client(&relay, "srv-sync-list", "conn_x").await;
    // Give the owner a moment to register the client.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut control = control(&relay, "srv-sync-list").await;
    let sync = recv_json(&mut control).await;
    assert_eq!(sync["type"], "sync");
    assert_eq!(sync["connectionIds"], serde_json::json!(["conn_x"]));
}

#[tokio::test]
async fn control_ping_receives_pong() {
    let relay = start_relay().await;
    let mut control = control(&relay, "srv-ping").await;
    assert_eq!(recv_json(&mut control).await["type"], "sync");

    send_text(&mut control, r#"{"type":"ping"}"#).await;
    let pong = recv_json(&mut control).await;
    assert_eq!(pong["type"], "pong");
    assert!(pong["ts"].is_number());
}

#[tokio::test]
async fn rejects_v1_and_bad_params_with_400() {
    let relay = start_relay().await;
    assert_eq!(connect_err(&relay, "serverId=x&role=client&v=1").await, 400);
    assert_eq!(connect_err(&relay, "serverId=x&role=client").await, 400);
    assert_eq!(connect_err(&relay, "serverId=x&v=2").await, 400);
    assert_eq!(connect_err(&relay, "role=client&v=2").await, 400);
    let long = "a".repeat(300);
    assert_eq!(
        connect_err(&relay, &format!("serverId={long}&role=client&v=2")).await,
        400
    );
}

#[tokio::test]
async fn forwards_client_frames_to_data_socket_in_order() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-fwd", "conn_a").await;
    let mut client_socket = client(&relay, "srv-fwd", "conn_a").await;

    send_text(&mut client_socket, "first").await;
    send_text(&mut client_socket, "second").await;
    send_binary(&mut client_socket, vec![1, 2, 3]).await;

    assert_eq!(recv_text(&mut data).await, "first");
    assert_eq!(recv_text(&mut data).await, "second");
    let frame = data.next().await.unwrap().unwrap();
    assert_eq!(frame.into_data(), vec![1u8, 2, 3]);
}

#[tokio::test]
async fn forwards_data_socket_frames_to_all_clients() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-bcast", "conn_a").await;
    let mut client_one = client(&relay, "srv-bcast", "conn_a").await;
    let mut client_two = client(&relay, "srv-bcast", "conn_a").await;
    // Both clients need to be attached before the data socket sends.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    send_text(&mut data, "to-clients").await;
    assert_eq!(recv_text(&mut client_one).await, "to-clients");
    assert_eq!(recv_text(&mut client_two).await, "to-clients");
}

#[tokio::test]
async fn isolates_connection_ids() {
    let relay = start_relay().await;
    let mut data_a = data_socket(&relay, "srv-iso", "conn_a").await;
    let mut data_b = data_socket(&relay, "srv-iso", "conn_b").await;
    let mut client_a = client(&relay, "srv-iso", "conn_a").await;
    let mut client_b = client(&relay, "srv-iso", "conn_b").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    send_text(&mut client_a, "from-a").await;
    send_text(&mut client_b, "from-b").await;
    assert_eq!(recv_text(&mut data_a).await, "from-a");
    assert_eq!(recv_text(&mut data_b).await, "from-b");

    send_text(&mut data_a, "reply-a").await;
    send_text(&mut data_b, "reply-b").await;
    assert_eq!(recv_text(&mut client_a).await, "reply-a");
    assert_eq!(recv_text(&mut client_b).await, "reply-b");
}

#[tokio::test]
async fn client_frame_waits_for_data_socket_attach() {
    let relay = start_relay().await;
    let mut client_socket = client(&relay, "srv-wait", "conn_a").await;
    send_text(&mut client_socket, "early").await;

    // Attach the data socket after the frame is already in flight.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut data = data_socket(&relay, "srv-wait", "conn_a").await;
    assert_eq!(recv_text(&mut data).await, "early");
}

#[tokio::test]
async fn new_control_socket_replaces_old_with_1008() {
    let relay = start_relay().await;
    let mut old_control = control(&relay, "srv-replace").await;
    assert_eq!(recv_json(&mut old_control).await["type"], "sync");

    let mut new_control = control(&relay, "srv-replace").await;
    let (code, reason) = recv_close(&mut old_control).await;
    assert_eq!(code, 1008);
    assert_eq!(reason, "Replaced by new connection");
    assert_eq!(recv_json(&mut new_control).await["type"], "sync");
}

#[tokio::test]
async fn new_data_socket_replaces_old_with_1008() {
    let relay = start_relay().await;
    let mut old_data = data_socket(&relay, "srv-replace-data", "conn_a").await;
    let _new_data = data_socket(&relay, "srv-replace-data", "conn_a").await;
    let (code, reason) = recv_close(&mut old_data).await;
    assert_eq!(code, 1008);
    assert_eq!(reason, "Replaced by new connection");
}

#[tokio::test]
async fn data_socket_disconnect_cascades_1012_to_clients() {
    let relay = start_relay().await;
    let data = data_socket(&relay, "srv-cascade", "conn_a").await;
    let mut client_socket = client(&relay, "srv-cascade", "conn_a").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    drop(data);
    let (code, reason) = recv_close(&mut client_socket).await;
    assert_eq!(code, 1012);
    assert_eq!(reason, "Server disconnected");
}

#[tokio::test]
async fn last_client_disconnect_closes_data_socket_with_1001() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-lastclient", "conn_a").await;
    let client_one = client(&relay, "srv-lastclient", "conn_a").await;
    let client_two = client(&relay, "srv-lastclient", "conn_a").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    drop(client_one);
    // Data socket must stay open while one client remains.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    drop(client_two);
    let (code, reason) = recv_close(&mut data).await;
    assert_eq!(code, 1001);
    assert_eq!(reason, "Client disconnected");
}

#[tokio::test]
async fn control_disconnect_has_no_cascade() {
    let relay = start_relay().await;
    let control_socket = control(&relay, "srv-noc").await;
    let mut data = data_socket(&relay, "srv-noc", "conn_a").await;
    let mut client_socket = client(&relay, "srv-noc", "conn_a").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    drop(control_socket);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Frame routing still works.
    send_text(&mut client_socket, "still-alive").await;
    assert_eq!(recv_text(&mut data).await, "still-alive");
}

#[tokio::test]
async fn session_survives_owner_reuse_across_reconnects() {
    let relay = start_relay().await;
    {
        let mut control = control(&relay, "srv-reuse").await;
        assert_eq!(recv_json(&mut control).await["type"], "sync");
    }
    // Reconnect to the same serverId immediately.
    let mut control = control(&relay, "srv-reuse").await;
    assert_eq!(recv_json(&mut control).await["type"], "sync");
}
