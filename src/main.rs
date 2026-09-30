use axum::{routing::{get, post}, Router, middleware as axum_middleware};
use std::net::SocketAddr;
use std::sync::Arc;

mod proxy;
mod middleware;

#[tokio::main] // transforms main into an async entry point
async fn main() {
    let rate_limiter_state = Arc::new(middleware::RateLimiter::new());
    let app = Router::new()
                .route("/", get(home_handler))
                .route("/api/stream", post(proxy::proxy_llm_stream))
                .layer(axum_middleware::from_fn_with_state(
                    rate_limiter_state,
                    middleware::guardrail_middleware
                ));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Agent backend listening on http://{}", addr);

    // active, async TCP listener from Tokio to capture incoming web traffic
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn home_handler() -> &'static str {
    "Enterprise App is Running."
}