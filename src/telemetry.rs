use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::mpsc;
use tokio::time;

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

    let mut cleanup_interval = time::interval(Duration::from_secs(30));
    let session_ttl_secs = 300; // 5 minutes max session life

    loop {
        tokio::select! {
            // 1st branch: process incoming events
            Some(event) = rx.recv() => {
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
                            ship_to_kafka(record, &producer, &kafka_topic);
                        }
                    }
                }
            }

            // 2nd branch: periodic TTL sweeper
            _ = cleanup_interval.tick() => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let mut expired_keys = Vec::new();

                for (req_id, record) in active_sessions.iter() {
                    if now - record.timestamp > session_ttl_secs {
                        expired_keys.push(req_id.clone());
                    }
                }

                for req_id in expired_keys {
                    if let Some(mut record) = active_sessions.remove(&req_id) {
                        record.full_response.push_str("\n[SYSTEM: TRUNCATED_DUE_TO_DISCONNECT]");
                        ship_to_kafka(record, &producer, &kafka_topic);
                    }
                }
            }

            // 3rd branch: shutdown if the channel closes
            else => {
                println!("Telemetry channel closed, shutting down worker.");
                break;
            }
        }
    }
}

fn ship_to_kafka(record: ChatAuditRecord, producer: &FutureProducer, topic: &str) {
    let producer = producer.clone();
    let topic = topic.to_string();

    tokio::spawn(async move {
        let payload = serde_json::to_string(&record).unwrap();
        let deliver_status = producer.send(FutureRecord::to(&topic).payload(&payload).key(&record.user_id), Timeout::Never).await;

        if let Err((kafka_err, _)) = deliver_status {
            eprintln!("Failed to enqueue telemetry for {}: {:?}", record.request_id, kafka_err);
        }
    });
}