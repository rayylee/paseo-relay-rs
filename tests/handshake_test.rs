mod common;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use common::*;

fn valid_key() -> String {
    let mut key = [0u8; 32];
    key[0] = 9; // X25519 base point
    BASE64.encode(key)
}

#[tokio::test]
async fn forwards_handshake_with_valid_key() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-hs-ok", "conn_a").await;
    let mut client_socket = client(&relay, "srv-hs-ok", "conn_a").await;

    let hello = format!(r#"{{"type":"hello","key":"{}"}}"#, valid_key());
    send_text(&mut client_socket, &hello).await;
    assert_eq!(recv_text(&mut data).await, hello);
}

#[tokio::test]
async fn rejects_invalid_key_with_1008_and_does_not_forward() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-hs-bad", "conn_a").await;
    let mut client_socket = client(&relay, "srv-hs-bad", "conn_a").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    send_text(
        &mut client_socket,
        r#"{"type":"e2ee_hello","key":"not-a-key"}"#,
    )
    .await;
    let (code, reason) = recv_close(&mut client_socket).await;
    assert_eq!(code, 1008);
    assert_eq!(reason, "Invalid handshake key");

    // Client disconnect cascades 1001 to the data socket; the rejected frame
    // must never arrive first.
    let (code, _reason) = recv_close(&mut data).await;
    assert_eq!(code, 1001);
}

#[tokio::test]
async fn rejects_weak_key() {
    let relay = start_relay().await;
    let _data = data_socket(&relay, "srv-hs-weak", "conn_a").await;
    let mut client_socket = client(&relay, "srv-hs-weak", "conn_a").await;

    let weak = BASE64.encode([0u8; 32]);
    send_text(
        &mut client_socket,
        &format!(r#"{{"type":"hello","key":"{weak}"}}"#),
    )
    .await;
    let (code, reason) = recv_close(&mut client_socket).await;
    assert_eq!(code, 1008);
    assert_eq!(reason, "Invalid handshake key");
}

#[tokio::test]
async fn non_handshake_json_is_forwarded_unchecked() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-hs-pass", "conn_a").await;
    let mut client_socket = client(&relay, "srv-hs-pass", "conn_a").await;

    let payload = r#"{"type":"data","key":"garbage-key"}"#;
    send_text(&mut client_socket, payload).await;
    assert_eq!(recv_text(&mut data).await, payload);
}

#[tokio::test]
async fn server_frames_are_not_handshake_checked() {
    let relay = start_relay().await;
    let mut data = data_socket(&relay, "srv-hs-server", "conn_a").await;
    let mut client_socket = client(&relay, "srv-hs-server", "conn_a").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // A "hello" with a bad key from the data socket must pass through.
    let payload = r#"{"type":"hello","key":"invalid"}"#;
    send_text(&mut data, payload).await;
    assert_eq!(recv_text(&mut client_socket).await, payload);
}
