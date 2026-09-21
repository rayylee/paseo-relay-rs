use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use include_dir::{Dir, include_dir};

const INDEX_HTML: &str = include_str!("web/index.html");
const NACL_JS: &str = include_str!("web/nacl-fast.js");

/// Built Paseo web app, vendored into the binary so a single relay artifact
/// can also serve the pairing UI. Regenerate with `scripts/vendor-web-ui.sh`.
static WEB_UI: Dir = include_dir!("$CARGO_MANIFEST_DIR/web-ui");

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

/// Serve the vendored Paseo web app with SPA fallback: unknown paths receive
/// `index.html` so the Expo router can resolve the route in the browser.
pub async fn web_ui(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(file) = WEB_UI.get_file(path) {
        return static_response(StatusCode::OK, content_type(path), file.contents());
    }

    match WEB_UI.get_file("index.html") {
        Some(index) => static_response(
            StatusCode::OK,
            HeaderValue::from_static("text/html; charset=utf-8"),
            index.contents(),
        ),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

fn content_type(path: &str) -> HeaderValue {
    mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream")
        .parse()
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"))
}

fn static_response(status: StatusCode, content_type: HeaderValue, body: &'static [u8]) -> Response {
    let mut response = Response::new(Body::from(axum::body::Bytes::from_static(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response
}
