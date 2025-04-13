# syntax=docker/dockerfile:1-labs
FROM rust:1.85-bookworm AS base
WORKDIR /app

FROM base AS planner
COPY ./Cargo.lock .
COPY ./Cargo.toml .

RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runner

WORKDIR /app
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y ca-certificates libssl-dev
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/artifact-downloader /app/executable
ENTRYPOINT ["/app/executable"]
