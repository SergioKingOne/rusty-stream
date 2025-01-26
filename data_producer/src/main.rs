use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use tokio::time::{self, Duration};

#[derive(Serialize, Deserialize)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // We create a FutureProducer (asynchronous) to send messages to Kafka/Redpanda.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:9092") // match docker-compose
        .create()?;

    let mut interval = time::interval(Duration::from_secs(2));
    let topic = "sensor-data";

    loop {
        interval.tick().await;

        let data = SensorData {
            sensor_id: "sensor-123".to_string(),
            value: rand::random::<f64>() * 100.0,
            timestamp: chrono::Utc::now().timestamp(),
        };

        let payload = serde_json::to_string(&data)?;

        // Send the message asynchronously
        match producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key("sensor-123"),
                Duration::from_secs(0),
            )
            .await
        {
            Ok(delivery) => {
                println!("Data sent: {:?}", delivery);
            }
            Err((err, _msg)) => {
                eprintln!("Error producing message: {}", err);
            }
        }
    }
}
