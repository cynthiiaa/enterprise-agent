# September 30, 2026

Currently, incoming requests are routed to the Axum pipeline. The request is first sent through the layers defined in `middleware.rs`. This process extracts the JSON prompt, runs a quick regex check to block prompt injections, and checks the user's rate limit. If it fails, it returns an HTTP error code.

If the guardrails pass, the request goes to the handler in p`proxy.rs`. The handler uses `reqwest` to send the payload to the backend LLM service (which is GPT-4o). As the LLM generates tokens, `reqwest` yields a stream of bytes. `proxy.rs` maps the byte stream into an `axum::response::sse::Event` stream and Axum automatically flushes these events tot he client connection.

While `proxy.rs` iterates over the stream, it clones the generated tokens and sends them down a `tokio::sync::mpsc` channel to `telemetry.rs`. The telemetry script can aggregate the final response and send it to Kafka asynchronously without delaying the client's stream.
