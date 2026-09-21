use axum::extract::ws::Message;

pub const MAXIMUM_FRAME_WIRE_BYTES: usize = 32 * 1024 * 1024;
pub const MAXIMUM_CLIENT_FRAME_HEADER_BYTES: usize = 14;

// One payload limit covers both individual frames and assembled fragmented
// messages. Derive it from the stricter masked client-frame contract so the
// 32 MiB wire ceiling is never exceeded.
pub const MAXIMUM_MESSAGE_PAYLOAD_BYTES: usize =
    MAXIMUM_FRAME_WIRE_BYTES - MAXIMUM_CLIENT_FRAME_HEADER_BYTES;
pub const MAXIMUM_CONTROL_PAYLOAD_BYTES: usize = 64 * 1024;

pub fn payload_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) => bytes.len(),
        Message::Ping(bytes) | Message::Pong(bytes) => bytes.len(),
        Message::Close(_) => 0,
    }
}
