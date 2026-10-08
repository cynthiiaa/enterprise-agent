use axum::{body::Body, extract::{Request, State}, http::StatusCode, middleware::Next, response::Response};
use std::{collections:HashMap, sync::{Arc, Mutex}, time::{Duration, Instant}};

pub struct RateLimiter {
    pub visits: Mutex<HashMap<String, (Instant, u32)>>
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            visits: Mutex::new(HashMap::new())
        }
    }
}

pub async fn check_quota(State(limiter): State<Arc<RateLimiter>>, request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
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

    Ok(next.run(request).await)
}