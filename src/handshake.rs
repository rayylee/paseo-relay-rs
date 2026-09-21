use base64::prelude::BASE64_STANDARD as BASE64;
use base64::Engine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeType {
    Hello,
    E2eeHello,
}

impl HandshakeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            HandshakeType::Hello => "hello",
            HandshakeType::E2eeHello => "e2ee_hello",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeResult {
    NotHandshake,
    Accept(HandshakeType),
    Reject(HandshakeType),
}

// Unsupported X25519 public-key encodings. The separate field check rejects
// invalid coordinates, including high-bit forms.
const UNSUPPORTED_PUBLIC_KEYS: [[u8; 32]; 7] = [
    hex(*b"0000000000000000000000000000000000000000000000000000000000000000"),
    hex(*b"0100000000000000000000000000000000000000000000000000000000000000"),
    hex(*b"E0EB7A7C3B41B8AE1656E3FAF19FC46ADA098DEB9C32B1FD866205165F49B800"),
    hex(*b"5F9C95BCA3508C24B1D0B1559C83EF5B04445CC4581C8E86D8224EDDD09F1157"),
    hex(*b"ECFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
    hex(*b"EDFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
    hex(*b"EEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
];

// 2^255 - 19 in little-endian bytes.
const FIELD_PRIME_LE: [u8; 32] =
    hex(*b"EDFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F");

/// Inspect a client frame: if it parses as a JSON handshake (`type` of
/// "hello"/"e2ee_hello"), validate the X25519 public key in `key`.
pub fn check(payload: &[u8]) -> HandshakeResult {
    let Ok(message) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return HandshakeResult::NotHandshake;
    };

    let handshake_type = match message.get("type").and_then(|value| value.as_str()) {
        Some("hello") => HandshakeType::Hello,
        Some("e2ee_hello") => HandshakeType::E2eeHello,
        _ => return HandshakeResult::NotHandshake,
    };

    if valid_public_key(message.get("key")) {
        HandshakeResult::Accept(handshake_type)
    } else {
        HandshakeResult::Reject(handshake_type)
    }
}

fn valid_public_key(value: Option<&serde_json::Value>) -> bool {
    let Some(encoded) = value.and_then(|value| value.as_str()) else {
        return false;
    };
    let Ok(public_key) = BASE64.decode(encoded) else {
        return false;
    };
    let Ok(public_key) = <[u8; 32]>::try_from(public_key) else {
        return false;
    };
    // Canonical padded Base64 (round-trip check).
    if BASE64.encode(public_key) != encoded {
        return false;
    }
    if !canonical_coordinate(&public_key) {
        return false;
    }
    !UNSUPPORTED_PUBLIC_KEYS.contains(&public_key)
}

// Little-endian 256-bit integer must be < 2^255 - 19.
fn canonical_coordinate(key: &[u8; 32]) -> bool {
    for index in (0..32).rev() {
        if key[index] != FIELD_PRIME_LE[index] {
            return key[index] < FIELD_PRIME_LE[index];
        }
    }
    false // equal to the prime is out of range
}

const fn hex_nibble(character: u8) -> u8 {
    match character {
        b'0'..=b'9' => character - b'0',
        b'A'..=b'F' => character - b'A' + 10,
        b'a'..=b'f' => character - b'a' + 10,
        _ => panic!("invalid hex digit"),
    }
}

const fn hex(text: [u8; 64]) -> [u8; 32] {
    let mut output = [0u8; 32];
    let mut index = 0;
    while index < 32 {
        output[index] = (hex_nibble(text[index * 2]) << 4) | hex_nibble(text[index * 2 + 1]);
        index += 1;
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello_with_key(key: &str) -> Vec<u8> {
        format!(r#"{{"type":"hello","key":"{key}"}}"#).into_bytes()
    }

    #[test]
    fn accepts_valid_key() {
        // 9 is the canonical X25519 base point; a valid coordinate.
        let mut key = [0u8; 32];
        key[0] = 9;
        let encoded = BASE64.encode(key);
        assert_eq!(
            check(&hello_with_key(&encoded)),
            HandshakeResult::Accept(HandshakeType::Hello)
        );
    }

    #[test]
    fn rejects_weak_keys() {
        for weak in UNSUPPORTED_PUBLIC_KEYS {
            let encoded = BASE64.encode(weak);
            assert_eq!(
                check(&hello_with_key(&encoded)),
                HandshakeResult::Reject(HandshakeType::Hello),
                "expected rejection for {encoded}"
            );
        }
    }

    #[test]
    fn rejects_non_canonical_base64_and_wrong_length() {
        assert_eq!(
            check(&hello_with_key("not-base64!")),
            HandshakeResult::Reject(HandshakeType::Hello)
        );
        let short = BASE64.encode([9u8; 16]);
        assert_eq!(
            check(&hello_with_key(&short)),
            HandshakeResult::Reject(HandshakeType::Hello)
        );
    }

    #[test]
    fn rejects_out_of_field_coordinate() {
        let mut key = [0xFFu8; 32];
        key[31] = 0x7F;
        key[0] = 0xEE; // 2^255 - 18, >= p — also happens to be blacklisted
        let encoded = BASE64.encode(key);
        assert_eq!(
            check(&hello_with_key(&encoded)),
            HandshakeResult::Reject(HandshakeType::Hello)
        );
        // Definitely above p and not in the blacklist:
        key[0] = 0xF0;
        let encoded = BASE64.encode(key);
        assert_eq!(
            check(&hello_with_key(&encoded)),
            HandshakeResult::Reject(HandshakeType::Hello)
        );
    }

    #[test]
    fn rejects_missing_key() {
        assert_eq!(
            check(br#"{"type":"e2ee_hello"}"#),
            HandshakeResult::Reject(HandshakeType::E2eeHello)
        );
    }

    #[test]
    fn passes_through_non_handshake() {
        assert_eq!(check(b"not json"), HandshakeResult::NotHandshake);
        assert_eq!(
            check(br#"{"type":"data","key":"zzz"}"#),
            HandshakeResult::NotHandshake
        );
    }
}
