use axum::{routing::{get, post}, Router, middleware as axum_middleware};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

mod proxy;
mod middleware;
mod telemetry;

use crate::telemetry::TelemetryEvent;

#[derive(Clone)]
pub struct AppState {
    pub telemetry_tx: mpsc::Sender<TelemetryEvent>
}

#[tokio::main] // transforms main into an async entry point
async fn main() {
    let (telemetry_tx, telemetry_rx) = tokio::sync::mpsc::channel::<TelemetryEvent>(50_000);
    tokio::spawn(async move {
        crate::telemetry::start_telemetry_worker(telemetry_rx, "localhost:9092", "llm_audit_logs".to_string()).await;
    });
    let state = AppState { telemetry_tx };
    let rate_limiter_state = Arc::new(middleware::RateLimiter::new());
    let app = Router::new()
                .route("/", get(home_handler))
                .route("/api/stream", post(proxy::proxy_llm_stream))
                .layer(axum_middleware::from_fn_with_state(
                    rate_limiter_state,
                    middleware::guardrail_middleware
                ))
                .with_state(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Agent backend listening on http://{}", addr);

    // active, async TCP listener from Tokio to capture incoming web traffic
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn home_handler() -> &'static str {
    "Enterprise App is Running."
}