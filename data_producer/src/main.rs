use anyhow::Result;
use async_trait::async_trait;
use rdkafka::{config::ClientConfig, producer::FutureProducer};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time;

#[derive(Serialize, Deserialize, Clone)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[async_trait]
trait EventProducer: Send + Sync {
    async fn send(&self, topic: &str, key: &str, payload: &str) -> Result<(), EventProducerError>;
}

#[async_trait]
trait DataGenerator: Send + Sync {
    fn generate(&self) -> SensorData;
}

#[derive(Debug, thiserror::Error)]
enum EventProducerError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

struct KafkaEventProducer {
    inner: FutureProducer,
}

impl KafkaEventProducer {
    fn new(bootstrap_servers: &str) -> Result<Self> {
        Ok(Self {
            inner: ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .create()?,
        })
    }
}

#[async_trait]
impl EventProducer for KafkaEventProducer {
    async fn send(&self, topic: &str, key: &str, payload: &str) -> Result<(), EventProducerError> {
        let record = rdkafka::producer::FutureRecord::to(topic)
            .payload(payload)
            .key(key);

        self.inner
            .send(record, Duration::from_secs(0))
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.into())
    }
}

struct SensorDataGeneratorImpl {
    sensor_id: String,
}

impl DataGenerator for SensorDataGeneratorImpl {
    fn generate(&self) -> SensorData {
        SensorData {
            sensor_id: self.sensor_id.clone(),
            value: rand::random::<f64>() * 100.0,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

struct ProducerConfig {
    topic: String,
    interval_seconds: u64,
    bootstrap_servers: String,
}

struct ProducerService {
    producer: Arc<dyn EventProducer>,
    generator: Arc<dyn DataGenerator>,
    config: ProducerConfig,
}

impl ProducerService {
    async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.interval_seconds));

        loop {
            interval.tick().await;
            let data = self.generator.generate();
            let payload = serde_json::to_string(&data)?;

            match self
                .producer
                .send(&self.config.topic, &data.sensor_id, &payload)
                .await
            {
                Ok(_) => println!("Sent data for {}", data.sensor_id),
                Err(e) => eprintln!("Failed to send data: {}", e),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ProducerConfig {
        topic: "sensor-data".to_string(),
        interval_seconds: 2,
        bootstrap_servers: "kafka:9092".to_string(),
    };

    let producer = Arc::new(KafkaEventProducer::new(&config.bootstrap_servers)?);
    let generator = Arc::new(SensorDataGeneratorImpl {
        sensor_id: "sensor-123".to_string(),
    });

    let service = ProducerService {
        producer,
        generator,
        config,
    };

    service.run().await
}
