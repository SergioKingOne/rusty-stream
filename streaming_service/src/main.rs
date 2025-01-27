use anyhow::Result;
use async_trait::async_trait;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::spawn;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[async_trait]
trait EventConsumer: Send + Sync {
    async fn consume(&self) -> Result<Option<String>, EventConsumerError>;
    async fn commit(&self) -> Result<(), EventConsumerError>;
}

#[async_trait]
trait SensorDataRepository: Send + Sync {
    async fn insert_sensor_data(&self, data: &SensorData) -> Result<(), StorageError>;
}

#[async_trait]
trait CacheRepository: Send + Sync {
    async fn cache_value(&self, key: &str, value: f64) -> Result<(), StorageError>;
}

struct KafkaEventConsumer {
    consumer: StreamConsumer,
}

impl KafkaEventConsumer {
    fn new(bootstrap_servers: &str, group_id: &str, topics: &[&str]) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .create()?;

        consumer.subscribe(topics)?;
        Ok(Self { consumer })
    }
}

#[async_trait]
impl EventConsumer for KafkaEventConsumer {
    async fn consume(&self) -> Result<Option<String>, EventConsumerError> {
        match self.consumer.recv().await {
            Ok(message) => {
                let payload = match message.payload_view::<str>() {
                    Some(Ok(s)) => Ok(Some(s.to_string())),
                    Some(Err(e)) => Err(EventConsumerError::Utf8(e)),
                    None => Ok(None),
                }?;
                Ok(payload)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn commit(&self) -> Result<(), EventConsumerError> {
        // Implementation would properly commit offsets
        Ok(())
    }
}

struct PostgresSensorRepository {
    pool: sqlx::PgPool,
}

impl PostgresSensorRepository {
    async fn new(db_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS sensor_data (
                id SERIAL PRIMARY KEY,
                sensor_id TEXT NOT NULL,
                value DOUBLE PRECISION,
                ts BIGINT
            )"#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl SensorDataRepository for PostgresSensorRepository {
    async fn insert_sensor_data(&self, data: &SensorData) -> Result<(), StorageError> {
        sqlx::query(r#"INSERT INTO sensor_data (sensor_id, value, ts) VALUES ($1, $2, $3)"#)
            .bind(&data.sensor_id)
            .bind(data.value)
            .bind(data.timestamp)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

struct RedisCacheRepository {
    client: redis::Client,
}

impl RedisCacheRepository {
    fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl CacheRepository for RedisCacheRepository {
    async fn cache_value(&self, key: &str, value: f64) -> Result<(), StorageError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.set::<_, _, ()>(key, value).await?;
        Ok(())
    }
}

struct MessageProcessor {
    consumer: Arc<dyn EventConsumer>,
    sensor_repo: Arc<dyn SensorDataRepository>,
    cache_repo: Arc<dyn CacheRepository>,
}

impl MessageProcessor {
    async fn process_message(&self, payload: &str) -> Result<()> {
        let sensor_data: SensorData = serde_json::from_str(payload)?;
        let sensor_data_clone = sensor_data.clone(); // Clone here before the spawns

        let sensor_repo = self.sensor_repo.clone();
        let cache_repo = self.cache_repo.clone();
        let sensor_id = sensor_data.sensor_id.clone();

        spawn(async move {
            if let Err(e) = sensor_repo.insert_sensor_data(&sensor_data).await {
                eprintln!("Failed to store in Postgres: {}", e);
            }
        });

        spawn(async move {
            let cache_key = format!("sensor:{}", sensor_id);
            if let Err(e) = cache_repo
                .cache_value(&cache_key, sensor_data_clone.value)
                .await
            {
                // Use the clone here
                eprintln!("Failed to cache in Redis: {}", e);
            }
        });

        Ok(())
    }

    async fn run(&self) -> Result<()> {
        loop {
            match self.consumer.consume().await {
                Ok(Some(payload)) => {
                    println!("Received message: {}", payload);
                    if let Err(e) = self.process_message(&payload).await {
                        eprintln!("Error processing message: {}", e);
                    }
                    self.consumer.commit().await?;
                }
                Ok(None) => continue,
                Err(e) => eprintln!("Consumer error: {}", e),
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum EventConsumerError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

#[derive(Debug, thiserror::Error)]
enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = Arc::new(KafkaEventConsumer::new(
        "127.0.0.1:9092",
        "streaming-service-group",
        &["sensor-data"],
    )?);

    let sensor_repo = Arc::new(
        PostgresSensorRepository::new("postgres://postgres:postgres@127.0.0.1:5432/rusty_stream")
            .await?,
    );

    let cache_repo = Arc::new(RedisCacheRepository::new("redis://127.0.0.1:6379")?);

    let processor = MessageProcessor {
        consumer,
        sensor_repo,
        cache_repo,
    };

    processor.run().await
}
