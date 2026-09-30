# Architect an Enterprise RAG System

### Core Streaming and Concurrency Engine
Need an async runtime that handles thousands of connections using minimal memory. Tokio is an event-driven architecture and Axum is a modern web framework.

We'll use the engine to act as a pipe. The engine opens a streaming connection to the backend LLM, reads chunks of data as they arrive, and immediately flushes them out tot the client using SSE.

## Running the App
⚠️ Make sure you've got an OPENAI_KEY or some LLM on the backend.

Open two separate terminals or use `tmux`
```bash
cargo run
```

Assuming you didn't run into any issues, use the other terminal to send a prompt
```bash
curl -v -N POST http://localhost:3000/api/stream \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Write a recipe on citrus desserts."}'
```