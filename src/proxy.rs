use axum::{response::sse::{Event, Sse}};
use futures_util::stream::{StreamExt, BoxStream};
use reqwest::Client;
use serde_json::json;
use eventsource_stream::Eventsource;
use axum::extract::State;
use crate::{AppState, TelemetryEvent};

pub async fn proxy_llm_stream(State(state): State<AppState>) -> Sse<BoxStream<'static, Result<Event, std::convert::Infallible>>> {
    let client = Client::new();

    let request_id = uuid::Uuid::new_v4().to_string();
    let prompt_content = "Write a recipe on citrus desserts.";
    let user_id = "anon_user";

    let _ = state.telemetry_tx.try_send(TelemetryEvent::Start {
        request_id: request_id.clone(),
        user_id: user_id.to_string(),
        prompt: prompt_content.to_string()
    });

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Write a recipe on citrus desserts."}],
        "stream": true
    });

    let response = client.post("https://api.openai.com/v1/chat/completions").bearer_auth(std::env::var("OPENAI_API_KEY").unwrap_or_default()).json(&payload).send().await;

    let response = match response {
        Ok(res) => res,
        Err(_) => {
            let _ = state.telemetry_tx.try_send(TelemetryEvent::Complete { request_id: request_id.clone()});
            return Sse::new(futures_util::stream::once(async {
                Ok(Event::default().data("Error connecting to LLM backend"))
            }).boxed());
        }
    };

    let byte_stream = response.bytes_stream().map(|result| {
        result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });

    let event_source = byte_stream.eventsource();

    let tx = state.telemetry_tx.clone();
    let stream_req_id = request_id.clone();

    let axum_stream = event_source.map(move |event_result| {
        match event_result {
            Ok(event) => {
                if event.data == "[DONE]" {
                    let _ = tx.try_send(TelemetryEvent::Complete { request_id: stream_req_id.clone() });
                } else {
                    let _ = tx.try_send(TelemetryEvent::Chunk { request_id: stream_req_id.clone(), text: event.data.clone() });
                }
                Ok(Event::default().data(event.data))
            }
            Err(_) => {
                let _ = tx.try_send(TelemetryEvent::Complete { request_id: stream_req_id.clone() });
                Ok(Event::default().data("[STREAM_ERROR]"))
            }
        }
    });

    let boxed_success: BoxStream<'static, Result<Event, std::convert::Infallible>> = axum_stream.boxed();
    Sse::new(boxed_success)
}