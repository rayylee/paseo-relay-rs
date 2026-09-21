#![allow(dead_code)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use paseo_relay::config::Config;
use paseo_relay::drain::Drain;
use paseo_relay::metrics::Metrics;
use paseo_relay::node::{self, Node, NodeOptions};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct Relay {
    pub node: Node,
    pub drain: Arc<Drain>,
    pub metrics: Arc<Metrics>,
    pub port: u16,
}

pub fn test_config() -> Config {
    Config {
        host: IpAddr::from([127, 0, 0, 1]),
        port: 0,
        ..Config::default()
    }
}

pub async fn start_relay() -> Relay {
    start_relay_with(test_config(), None).await
}

pub async fn start_relay_with(
    config: Config,
    memory_probe: Option<paseo_relay::capacity::MemoryProbe>,
) -> Relay {
    let drain = Arc::new(Drain::new(config.drain));
    let metrics = Arc::new(Metrics::default());
    let node = node::start(NodeOptions {
        config,
        drain: drain.clone(),
        metrics: metrics.clone(),
        memory_probe,
    })
    .await
    .expect("bind relay listener");
    let port = node.local_addr.port();
    Relay {
        node,
        drain,
        metrics,
        port,
    }
}

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(relay: &Relay, query: &str) -> Socket {
    let (socket, _response) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/ws?{query}", relay.port))
            .await
            .expect("websocket upgrade");
    socket
}

pub async fn connect_err(relay: &Relay, query: &str) -> u16 {
    match tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/ws?{query}", relay.port))
        .await
    {
        Ok(_) => panic!("expected upgrade rejection"),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => response.status().as_u16(),
        Err(other) => panic!("unexpected error: {other}"),
    }
}

pub async fn control(relay: &Relay, server_id: &str) -> Socket {
    connect(relay, &format!("serverId={server_id}&role=server&v=2")).await
}

pub async fn data_socket(relay: &Relay, server_id: &str, connection_id: &str) -> Socket {
    connect(
        relay,
        &format!("serverId={server_id}&role=server&v=2&connectionId={connection_id}"),
    )
    .await
}

pub async fn client(relay: &Relay, server_id: &str, connection_id: &str) -> Socket {
    connect(
        relay,
        &format!("serverId={server_id}&role=client&v=2&connectionId={connection_id}"),
    )
    .await
}

/// Receive the next text frame within a timeout, panicking on close/timeout.
pub async fn recv_text(socket: &mut Socket) -> String {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("timed out waiting for frame")
        .expect("stream ended")
        .expect("websocket error");
    match frame {
        Message::Text(text) => text.to_string(),
        other => panic!("expected text frame, got {other:?}"),
    }
}

pub async fn recv_json(socket: &mut Socket) -> serde_json::Value {
    serde_json::from_str(&recv_text(socket).await).expect("valid JSON")
}

/// Receive frames until a close frame arrives; returns (code, reason).
pub async fn recv_close(socket: &mut Socket) -> (u16, String) {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .expect("timed out waiting for close")
            .expect("stream ended without close frame");
        match frame {
            Ok(Message::Close(Some(CloseFrame { code, reason }))) => {
                return (close_code(code), reason.to_string());
            }
            Ok(Message::Close(None)) => return (1005, String::new()),
            Ok(_other) => continue,
            Err(tokio_tungstenite::tungstenite::Error::Protocol(_)) => {
                return (1006, String::new())
            }
            Err(error) => panic!("websocket error while waiting for close: {error}"),
        }
    }
}

pub async fn send_text(socket: &mut Socket, text: &str) {
    socket
        .send(Message::Text(text.into()))
        .await
        .expect("send text");
}

pub async fn send_binary(socket: &mut Socket, bytes: Vec<u8>) {
    socket
        .send(Message::Binary(bytes.into()))
        .await
        .expect("send binary");
}

fn close_code(code: CloseCode) -> u16 {
    code.into()
}

pub async fn http_get(relay: &Relay, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(("127.0.0.1", relay.port))
        .await
        .expect("connect");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.expect("read");
    let response = String::from_utf8_lossy(&response).to_string();
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("status line");
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_headers, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}
