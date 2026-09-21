use rand::Rng;

const MAX_ROUTE_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

/// A validated v2 relay connection identity. The v1 protocol is intentionally
/// unsupported: requests with `v=1` (or no `v`) are rejected at upgrade time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub server_id: String,
    pub role: Role,
    pub connection_id: String,
}

impl Connection {
    pub fn is_control(&self) -> bool {
        self.role == Role::Server && self.connection_id.is_empty()
    }

    pub fn from_query(query: &str) -> Result<Connection, &'static str> {
        let mut server_id = None;
        let mut role = None;
        let mut version = None;
        let mut connection_id = None;

        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "serverId" => server_id = Some(value.into_owned()),
                "role" => role = Some(value.into_owned()),
                "v" => version = Some(value.into_owned()),
                "connectionId" => connection_id = Some(value.into_owned()),
                _ => {}
            }
        }

        let role = match role.as_deref() {
            Some("server") => Role::Server,
            Some("client") => Role::Client,
            _ => return Err("Missing or invalid role parameter"),
        };

        let server_id = match server_id {
            Some(value) if !value.is_empty() && value.len() <= MAX_ROUTE_ID_BYTES => value,
            Some(value) if value.len() > MAX_ROUTE_ID_BYTES => return Err("serverId is too long"),
            _ => return Err("Missing serverId parameter"),
        };

        match version.as_deref().map(str::trim) {
            Some("2") => {}
            _ => return Err("Invalid protocol version"),
        }

        let connection_id = connection_id.as_deref().unwrap_or("").trim().to_string();
        if connection_id.len() > MAX_ROUTE_ID_BYTES {
            return Err("connectionId is too long");
        }
        let connection_id = if role == Role::Client && connection_id.is_empty() {
            generated_connection_id()
        } else {
            connection_id
        };

        Ok(Connection {
            server_id,
            role,
            connection_id,
        })
    }
}

fn generated_connection_id() -> String {
    let mut bytes = [0u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    let mut id = String::with_capacity(5 + 16);
    id.push_str("conn_");
    for byte in bytes {
        id.push_str(&format!("{byte:02x}"));
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_control_socket() {
        let connection = Connection::from_query("serverId=abc&role=server&v=2").unwrap();
        assert!(connection.is_control());
        assert_eq!(connection.server_id, "abc");
    }

    #[test]
    fn parses_data_socket() {
        let connection =
            Connection::from_query("serverId=abc&role=server&v=2&connectionId=conn_1").unwrap();
        assert!(!connection.is_control());
        assert_eq!(connection.connection_id, "conn_1");
    }

    #[test]
    fn generates_client_connection_id() {
        let connection = Connection::from_query("serverId=abc&role=client&v=2").unwrap();
        assert!(connection.connection_id.starts_with("conn_"));
        assert_eq!(connection.connection_id.len(), 5 + 16);
    }

    #[test]
    fn rejects_v1_and_missing_version() {
        assert_eq!(
            Connection::from_query("serverId=abc&role=client&v=1"),
            Err("Invalid protocol version")
        );
        assert_eq!(
            Connection::from_query("serverId=abc&role=client"),
            Err("Invalid protocol version")
        );
    }

    #[test]
    fn rejects_missing_role_and_server_id() {
        assert!(Connection::from_query("serverId=abc&v=2").is_err());
        assert!(Connection::from_query("role=client&v=2").is_err());
    }

    #[test]
    fn rejects_overlong_ids() {
        let long = "a".repeat(257);
        assert_eq!(
            Connection::from_query(&format!("serverId={long}&role=client&v=2")),
            Err("serverId is too long")
        );
        assert_eq!(
            Connection::from_query(&format!("serverId=abc&role=client&v=2&connectionId={long}")),
            Err("connectionId is too long")
        );
    }
}
