use axum::{body::Body, http::{Request, StatusCode}, middleware::Next, response::Response};
use regex::Regex;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Instant, Duration};

#[derive(Deserialize)]
struct PromptPayload {
    prompt: String
}

pub struct RateLimiter {
    visits: Mutex<HashMap<String, (Instant, u32)>>
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            visits: Mutex::new(HashMap::new())
        }
    }
}

pub async fn guardrail_middleware(
    axum::extract::State(limiter): axum::extract::State<Arc<RateLimiter>>,
    request: Request<Body>,
    next: Next
) -> Result<Response, StatusCode> {
    let client_ip = request.headers().get("x-forwarded-for").and_then(|v| v.to_str().ok()).unwrap_or("127.0.0.1").to_string();

    {
        let mut visits = limiter.visits.lock().unwrap();
        let now = Instant::now();
        let (last_visit, count) = visits.entry(client_ip).or_insert((now, 0));

        if now.duration_since(*last_visit) > Duration::from_secs(60) {
            *last_visit = now;
            *count = 0;
        }

        *count += 1;
        if *count > 20 {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }

    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.map_err(|_| StatusCode::BAD_REQUEST)?;

    if let Ok(payload) = serde_json::from_slice::<PromptPayload>(&bytes) {
        let safety_regex = Regex::new(r"(?i)(ignore previous instructions|reveal system prompt|restructed_word)").unwrap();

        if safety_regex.is_match(&payload.prompt) {
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    let reconstructed_request = Request::from_parts(parts, Body::from(bytes));

    Ok(next.run(reconstructed_request).await)
}