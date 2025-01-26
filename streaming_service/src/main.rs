use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tokio::spawn;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1) Initialize Postgres Pool
    let db_url = "postgres://postgres:postgres@postgres:5432/mydb";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await?;

    // We can ensure the table exists (very minimal example)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sensor_data (
            id SERIAL PRIMARY KEY,
            sensor_id TEXT NOT NULL,
            value DOUBLE PRECISION,
            ts BIGINT
        )
        "#,
    )
    .execute(&pool)
    .await?;

    // 2) Initialize Redis Client
    let redis_client = redis::Client::open("redis://redis:6379")?;

    // 3) Set up Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "streaming-service-group")
        .set("bootstrap.servers", "kafka:9092")
        .set("enable.partition.eof", "false")
        .create()?;

    consumer.subscribe(&["sensor-data"])?;

    // 4) Continuous stream consumption
    loop {
        match consumer.recv().await {
            Err(err) => {
                eprintln!("Kafka error: {}", err);
            }
            Ok(m) => {
                if let Some(payload) = m.payload_view::<str>() {
                    match payload {
                        Ok(json_str) => {
                            if let Ok(sensor_data) = serde_json::from_str::<SensorData>(json_str) {
                                // Process the data:
                                spawn(store_in_postgres(pool.clone(), sensor_data.clone()));
                                spawn(cache_in_redis(redis_client.clone(), sensor_data));
                            } else {
                                eprintln!("Failed to deserialize JSON: {}", json_str);
                            }
                        }
                        Err(_e) => {
                            eprintln!("Error reading payload as str");
                        }
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

async fn store_in_postgres(pool: sqlx::Pool<sqlx::Postgres>, data: SensorData) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO sensor_data (sensor_id, value, ts)
            VALUES ($1, $2, $3)
        "#,
    )
    .bind(data.sensor_id)
    .bind(data.value)
    .bind(data.timestamp)
    .execute(&pool)
    .await?;

    Ok(())
}

async fn cache_in_redis(client: redis::Client, data: SensorData) -> Result<(), anyhow::Error> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let cache_key = format!("sensor:{}", data.sensor_id);
    conn.set::<_, _, ()>(cache_key, data.value).await?;
    Ok(())
}
