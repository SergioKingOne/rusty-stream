# Multi-stage build
FROM rust:1.84.0 as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/data_producer /usr/local/bin/data_producer
ENTRYPOINT ["/usr/local/bin/data_producer"]
