use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;

use crate::protocol;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub drain: bool,
    pub max_connections: usize,
    pub http_idle_timeout_ms: u64,
    pub capacity_mutation_timeout_ms: u64,
    pub ingress_budget_bytes: usize,
    pub ingress_weight: usize,
    pub delivery_timeout_ms: u64,
    pub transport_send_timeout_ms: u64,
    pub control_queue_bytes: usize,
    pub data_attach_timeout_ms: u64,
    pub tcp_receive_buffer_bytes: usize,
    pub memory_watermark_bytes: usize,
    pub web_ui_dir: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::from([127, 0, 0, 1]),
            port: 4000,
            drain: false,
            max_connections: 20_000,
            http_idle_timeout_ms: 15_000,
            capacity_mutation_timeout_ms: 5_000,
            ingress_budget_bytes: 512 * 1024 * 1024,
            ingress_weight: 4,
            delivery_timeout_ms: 30_000,
            transport_send_timeout_ms: 35_000,
            control_queue_bytes: 1024 * 1024,
            data_attach_timeout_ms: 15_000,
            tcp_receive_buffer_bytes: 64 * 1024,
            memory_watermark_bytes: 0,
            web_ui_dir: None,
        }
    }
}

impl Config {
    pub fn load(environment: &HashMap<String, String>) -> Result<Config, String> {
        let defaults = Config::default();

        let host = match environment.get("PASEO_RELAY_HOST") {
            None => defaults.host,
            Some(value) => value
                .parse::<IpAddr>()
                .map_err(|_| "PASEO_RELAY_HOST must be an IP address".to_string())?,
        };

        let config = Config {
            host,
            port: integer(
                environment,
                "PASEO_RELAY_PORT",
                defaults.port as i64,
                1,
                65_535,
            )? as u16,
            drain: boolean(environment, "PASEO_RELAY_DRAIN", defaults.drain)?,
            max_connections: integer(
                environment,
                "PASEO_RELAY_MAX_CONNECTIONS",
                defaults.max_connections as i64,
                1,
                1_000_000,
            )? as usize,
            http_idle_timeout_ms: integer(
                environment,
                "PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS",
                defaults.http_idle_timeout_ms as i64,
                100,
                120_000,
            )? as u64,
            capacity_mutation_timeout_ms: integer(
                environment,
                "PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS",
                defaults.capacity_mutation_timeout_ms as i64,
                100,
                120_000,
            )? as u64,
            ingress_budget_bytes: integer(
                environment,
                "PASEO_RELAY_INGRESS_BUDGET_BYTES",
                defaults.ingress_budget_bytes as i64,
                128 * 1024 * 1024,
                8 * 1024 * 1024 * 1024,
            )? as usize,
            ingress_weight: integer(
                environment,
                "PASEO_RELAY_INGRESS_WEIGHT",
                defaults.ingress_weight as i64,
                1,
                16,
            )? as usize,
            delivery_timeout_ms: integer(
                environment,
                "PASEO_RELAY_DELIVERY_TIMEOUT_MS",
                defaults.delivery_timeout_ms as i64,
                100,
                120_000,
            )? as u64,
            transport_send_timeout_ms: integer(
                environment,
                "PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS",
                defaults.transport_send_timeout_ms as i64,
                100,
                300_000,
            )? as u64,
            control_queue_bytes: integer(
                environment,
                "PASEO_RELAY_CONTROL_QUEUE_BYTES",
                defaults.control_queue_bytes as i64,
                64,
                64 * 1024 * 1024,
            )? as usize,
            data_attach_timeout_ms: integer(
                environment,
                "PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS",
                defaults.data_attach_timeout_ms as i64,
                1_000,
                120_000,
            )? as u64,
            tcp_receive_buffer_bytes: integer(
                environment,
                "PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES",
                defaults.tcp_receive_buffer_bytes as i64,
                4 * 1024,
                1024 * 1024,
            )? as usize,
            memory_watermark_bytes: disabled_or_integer(
                environment,
                "PASEO_RELAY_MEMORY_WATERMARK_BYTES",
                defaults.memory_watermark_bytes as i64,
                256 * 1024 * 1024,
                64 * 1024 * 1024 * 1024,
            )? as usize,
            web_ui_dir: match environment.get("PASEO_RELAY_WEB_UI_DIR") {
                None => None,
                Some(value) if value.trim().is_empty() => None,
                Some(value) => Some(PathBuf::from(value)),
            },
        };

        validate_ingress_budget(config.ingress_budget_bytes, config.ingress_weight)?;
        validate_delivery_timeouts(config.delivery_timeout_ms, config.transport_send_timeout_ms)?;
        Ok(config)
    }

    pub fn load_from_env() -> Result<Config, String> {
        Config::load(&std::env::vars().collect())
    }
}

fn integer(
    environment: &HashMap<String, String>,
    key: &str,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, String> {
    match environment.get(key) {
        None => Ok(default),
        Some(value) => match value.parse::<i64>() {
            Ok(parsed) if (minimum..=maximum).contains(&parsed) => Ok(parsed),
            _ => Err(format!(
                "{key} must be an integer between {minimum} and {maximum}"
            )),
        },
    }
}

fn boolean(
    environment: &HashMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool, String> {
    match environment.get(key).map(String::as_str) {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(format!("{key} must be true or false")),
    }
}

fn disabled_or_integer(
    environment: &HashMap<String, String>,
    key: &str,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, String> {
    match environment.get(key).map(String::as_str) {
        None => Ok(default),
        Some("0") => Ok(0),
        Some(_) => integer(environment, key, default, minimum, maximum),
    }
}

fn validate_ingress_budget(budget: usize, weight: usize) -> Result<(), String> {
    if budget >= protocol::MAXIMUM_MESSAGE_PAYLOAD_BYTES * weight {
        Ok(())
    } else {
        Err(
            "PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message \
             at the configured weight"
                .to_string(),
        )
    }
}

fn validate_delivery_timeouts(delivery_ms: u64, transport_send_ms: u64) -> Result<(), String> {
    if delivery_ms < transport_send_ms {
        Ok(())
    } else {
        Err(
            "PASEO_RELAY_DELIVERY_TIMEOUT_MS must be lower than PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS"
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn defaults_load() {
        let config = Config::load(&env(&[])).unwrap();
        assert_eq!(config.port, 4000);
        assert_eq!(config.ingress_budget_bytes, 512 * 1024 * 1024);
        assert_eq!(config.memory_watermark_bytes, 0);
    }

    #[test]
    fn rejects_invalid_host() {
        assert!(Config::load(&env(&[("PASEO_RELAY_HOST", "not-an-ip")])).is_err());
    }

    #[test]
    fn rejects_delivery_timeout_at_or_above_transport_send_timeout() {
        let error = Config::load(&env(&[
            ("PASEO_RELAY_DELIVERY_TIMEOUT_MS", "35000"),
            ("PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS", "35000"),
        ]));
        assert!(error.is_err());
    }

    #[test]
    fn watermark_zero_disables_shedding() {
        let config = Config::load(&env(&[("PASEO_RELAY_MEMORY_WATERMARK_BYTES", "0")])).unwrap();
        assert_eq!(config.memory_watermark_bytes, 0);
    }
}
