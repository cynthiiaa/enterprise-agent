# September 30, 2026

Currently, incoming requests are routed to the Axum pipeline. The request is first sent through the layers defined in `middleware.rs`. This process extracts the JSON prompt, runs a quick regex check to block prompt injections, and checks the user's rate limit. If it fails, it returns an HTTP error code.

If the guardrails pass, the request goes to the handler in `proxy.rs`. The handler uses `reqwest` to send the payload to the backend LLM service (which is GPT-4o). As the LLM generates tokens, `reqwest` yields a stream of bytes. `proxy.rs` maps the byte stream into an `axum::response::sse::Event` stream and Axum automatically flushes these events tot he client connection.

While `proxy.rs` iterates over the stream, it clones the generated tokens and sends them down a `tokio::sync::mpsc` channel to `telemetry.rs`. The telemetry script can aggregate the final response and send it to Kafka asynchronously without delaying the client's stream.

# October 07, 2026

Built out the `telemetry.rs` script. Since the LLM proxy is streaming data back to the user via SSE, the telemetry worker receives **chunks** (partial stream pieces) overtime. The script puts these pieces together into a complete response and pushes the full prompt/response pair to a Kafka topic.

Depending on the event received, there are three different actions to take. When the session is `Complete`, a new asynchronous tokio task spaws. Inside this task, the complete `ChatAuditRecord` is serialized into JSON and the JSON payload is sent to a specific Kafka topic using the `user_id` as the Kafka partition key.

I'm also in the process of splitting up `src/middleware.rs`. There's a `rate_limit.rs` script that implements a simple in-memory rate limiter.