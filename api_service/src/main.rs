use anyhow::Result;
use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::FromRow;
use std::{net::SocketAddr, sync::Arc};

#[derive(Serialize, Deserialize, Clone, FromRow)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[async_trait]
trait SensorDataStore: Send + Sync {
    async fn get_latest(&self) -> Result<Vec<SensorData>, anyhow::Error>;
    async fn insert(&self, data: &SensorData) -> Result<(), anyhow::Error>;
}

#[async_trait]
trait CacheStore: Send + Sync {
    async fn get_recent(&self, sensor_id: &str) -> Result<Option<f64>, anyhow::Error>;
}

struct PostgresSensorDataStore {
    pool: sqlx::PgPool,
}

#[async_trait]
impl SensorDataStore for PostgresSensorDataStore {
    async fn get_latest(&self) -> Result<Vec<SensorData>, anyhow::Error> {
        let rows = sqlx::query_as::<_, SensorData>(
            "SELECT sensor_id, value, ts as timestamp FROM sensor_data ORDER BY id DESC LIMIT 10",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn insert(&self, data: &SensorData) -> Result<(), anyhow::Error> {
        sqlx::query("INSERT INTO sensor_data (sensor_id, value, ts) VALUES ($1, $2, $3)")
            .bind(&data.sensor_id)
            .bind(data.value)
            .bind(data.timestamp)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

struct RedisCacheStore {
    client: redis::Client,
}

#[async_trait]
impl CacheStore for RedisCacheStore {
    async fn get_recent(&self, sensor_id: &str) -> Result<Option<f64>, anyhow::Error> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let cache_key = format!("sensor:{}", sensor_id);
        let value: Option<f64> = conn.get(cache_key).await?;
        Ok(value)
    }
}

#[derive(Clone)]
struct AppState {
    sensor_data: Arc<dyn SensorDataStore + Send + Sync>,
    cache: Arc<dyn CacheStore + Send + Sync>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let db_url = "postgres://postgres:postgres@postgres:5432/mydb";
    let pool = PgPoolOptions::new().connect(db_url).await?;

    let redis_client = redis::Client::open("redis://redis:6379")?;

    let state = AppState {
        sensor_data: Arc::new(PostgresSensorDataStore { pool }),
        cache: Arc::new(RedisCacheStore {
            client: redis_client,
        }),
    };

    let app = Router::new()
        .route("/data", get(get_data))
        .route("/recent/:sensor_id", get(get_recent))
        .route("/data", post(post_data))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("API server listening on {}", addr);
    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn get_data(State(state): State<AppState>) -> Json<Vec<SensorData>> {
    state
        .sensor_data
        .get_latest()
        .await
        .unwrap_or_default()
        .into()
}

async fn get_recent(
    State(state): State<AppState>,
    Path(sensor_id): Path<String>,
) -> Json<Option<f64>> {
    state
        .cache
        .get_recent(&sensor_id)
        .await
        .unwrap_or(None)
        .into()
}

async fn post_data(State(state): State<AppState>, Json(payload): Json<SensorData>) -> Json<String> {
    let _ = state.sensor_data.insert(&payload).await;
    format!("Inserted sensor data for sensor_id: {}", payload.sensor_id).into()
}
