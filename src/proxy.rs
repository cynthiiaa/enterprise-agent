use axum::{response::sse::{Event, Sse}};
use futures_util::stream::{StreamExt, BoxStream};
use reqwest::Client;
use serde_json::json;
use std::convert::Infallible;
use eventsource_stream::Eventsource;

pub async fn proxy_llm_stream() -> Sse<BoxStream<'static, Result<Event, std::convert::Infallible>>> {
    let client = Client::new();

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Write a recipe on citrus desserts."}],
        "stream": true
    });

    let response = client.post("https://openai.com").bearer_auth(std::env::var("OPENAI_API_KEY").unwrap_or_default()).json(&payload).send().await;

    let response = match response {
        Ok(res) => res,
        Err(_) => return Sse::new(futures_util::stream::once(async {
            Ok(Event::default().data("Error connecting to LLM backend"))
        }).boxed()),
    };

    let byte_stream = response.bytes_stream().map(|result| {
        result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });

    let event_source = byte_stream.eventsource();

    let axum_stream = event_source.map(|event_result| {
        match event_result {
            Ok(event) => {
                Ok(Event::default().data(event.data))
            }
            Err(_) => {
                Ok(Event::default().data("[STREAM_ERROR]"))
            }
        }
    });

    let boxed_success: BoxStream<'static, Result<Event, std::convert::Infallible>> = axum_stream.boxed();
    Sse::new(boxed_success)
}