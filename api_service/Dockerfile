# Multi-stage build
FROM rust:1.84.0 as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/api_service /usr/local/bin/api_service
ENTRYPOINT ["/usr/local/bin/api_service"]
