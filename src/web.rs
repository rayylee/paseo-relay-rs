use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};

const INDEX_HTML: &str = include_str!("web/index.html");
const NACL_JS: &str = include_str!("web/nacl-fast.js");

/// Embedded debug pairing console. Consumes the `#offer=` URL fragment in the
/// browser, connects to the relay's own `/ws` endpoint as a client, and runs
/// the Paseo E2EE handshake so the daemon can be reached without app.paseo.sh.
pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Embedded tweetnacl used by the console's NaCl box encryption.
pub async fn nacl_fast() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        NACL_JS,
    )
        .into_response()
}
