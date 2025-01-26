use anyhow::Result;
use axum::{
    extract::Path,
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::FromRow;
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Clone, FromRow)]
struct SensorData {
    sensor_id: String,
    value: f64,
    timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let db_url = "postgres://postgres:postgres@postgres:5432/mydb";
    let pool = PgPoolOptions::new().connect(db_url).await?;

    let redis_client = redis::Client::open("redis://redis:6379")?;

    let app = Router::new()
        .route(
            "/data",
            get({
                let pool = pool.clone();
                move || get_data(pool.clone())
            }),
        )
        .route(
            "/recent/:sensor_id",
            get({
                let redis_client = redis_client.clone();
                move |Path(sensor_id): Path<String>| get_recent(redis_client.clone(), sensor_id)
            }),
        )
        .route(
            "/data",
            post({
                let pool = pool.clone();
                move |Json(payload): Json<SensorData>| post_data(pool.clone(), payload)
            }),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("API server listening on {}", addr);
    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn get_data(pool: sqlx::Pool<sqlx::Postgres>) -> Json<Vec<SensorData>> {
    let rows = sqlx::query_as::<_, SensorData>(
        "SELECT sensor_id, value, ts as timestamp FROM sensor_data ORDER BY id DESC LIMIT 10",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    Json(rows)
}

async fn get_recent(client: redis::Client, sensor_id: String) -> Json<Option<f64>> {
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let cache_key = format!("sensor:{}", sensor_id);
    let value: Option<f64> = conn.get(cache_key).await.unwrap_or(None);
    Json(value)
}

async fn post_data(pool: sqlx::Pool<sqlx::Postgres>, payload: SensorData) -> Json<String> {
    // Insert into DB
    let _ = sqlx::query("INSERT INTO sensor_data (sensor_id, value, ts) VALUES ($1, $2, $3)")
        .bind(&payload.sensor_id)
        .bind(payload.value)
        .bind(payload.timestamp)
        .execute(&pool)
        .await;

    Json(format!(
        "Inserted sensor data for sensor_id: {}",
        payload.sensor_id
    ))
}
