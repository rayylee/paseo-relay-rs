use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::capacity::Admission;
use crate::node::AppState;

pub async fn health() -> Response {
    json(StatusCode::OK, r#"{"status":"ok"}"#)
}

pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    if is_ready(&state).await {
        json(StatusCode::OK, r#"{"status":"ready"}"#)
    } else {
        json(StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"unready"}"#)
    }
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let status = state.capacity.status().await;
    let ready = !state.drain.draining()
        && matches!(status.map(|status| status.admission), Some(Admission::Open));
    let body = state.metrics.render(
        ready,
        state.drain.draining(),
        status.map(|status| status.gauges),
        state.ownership.active_sessions() as u64,
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

pub async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain")],
        "not found\n",
    )
        .into_response()
}

async fn is_ready(state: &AppState) -> bool {
    if state.drain.draining() {
        return false;
    }
    matches!(
        state.capacity.status().await.map(|status| status.admission),
        Some(Admission::Open)
    )
}

fn json(status: StatusCode, body: &'static str) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}
