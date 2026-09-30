# Architect an Enterprise RAG System

### Core Streaming and Concurrency Engine
Need async runtime that handles thousands of connections using minimal memory. Tokio is an event-driven architecture and Axum is a modern web framework.

We'll use the engine to act as a pipe. The engine opens a streaming connection to the backend LLM, reads chunks of data as they arrive, and immediately flushes them out tot the client using SSE.