use axum::{body::Body, extract::Request, http::StatusCode, middleware::Next, response::Response};
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;

#[derive(Deserialize)]
struct PromptPayload {
    prompt: String
}

static SAFETY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(ignore previous instructions|reveal system prompt|restructed_word)").unwrap()
});

pub async fn inspect_prompt(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.map_err(|_| StatusCode::BAD_REQUEST)?;

    if let Ok(payload) = serde_json::from_slice::<PromptPayload>(&bytes) {
        if SAFETY_REGEX.is_match(&payload.prompt) {
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    let reconstructed_request = Request::from_parts(parts, Body::from(bytes));
    Ok(next.run(reconstructed_request).await)
}