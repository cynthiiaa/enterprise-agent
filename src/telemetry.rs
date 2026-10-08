use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    Start {
        request_id: String,
        user_id: String,
        prompt: String
    },
    Chunk {
        request_id: String,
        text: String
    },
    Complete {
        request_id: String
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatAuditRecord {
    pub request_id: String,
    pub user_id: String,
    pub prompt: String,
    pub full_response: String,
    pub timestamp: u64
}

pub async fn start_telemetry_worker(mut rx: mpsc::Receiver<TelemetryEvent>, kafka_brokers: &str, kafka_topic: String) {
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", kafka_brokers)
                                                      .set("message.timeout.ms", "5000")
                                                      .set("queue.buffering.max.messages", "100000")
                                                      .create()
                                                      .expect("Failed to create Kafka producer");
    
    let mut active_sessions: HashMap<String, ChatAuditRecord> = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            TelemetryEvent::Start { request_id, user_id, prompt } => {
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                active_sessions.insert(request_id.clone(), ChatAuditRecord {
                                                                request_id,
                                                                user_id,
                                                                prompt,
                                                                full_response: String::with_capacity(2048),
                                                                timestamp
                });
            }
            TelemetryEvent::Chunk { request_id, text } => {
                if let Some(session) = active_sessions.get_mut(&request_id) {
                    session.full_response.push_str(&text);
                }
            }
            TelemetryEvent::Complete { request_id } => {
                if let Some(record) = active_sessions.remove(&request_id) {
                    let producer = producer.clone();
                    let topic = kafka_topic.clone();

                    tokio::spawn(async move {
                        let payload = serde_json::to_string(&record).unwrap();
                        let delivery_status = producer.send(FutureRecord::to(&topic).payload(&payload).key(&record.user_id), Timeout::Never).await;

                        if let Err((kafka_err, _)) = delivery_status {
                            eprintln!("Failed to enqueue telemetry for {}: {:?}", record.request_id, kafka_err);
                        }
                    });
                }
            }
        }
    }
}