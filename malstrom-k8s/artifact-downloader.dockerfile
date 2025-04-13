FROM rust:1.85-alpine3.21 AS base
RUN apk add protoc musl-dev openssl-dev openssl-libs-static
RUN cargo install --locked cargo-chef && rm -rf ~/.cargo/registry/cache

FROM base AS planner
WORKDIR /artifact-downloader
COPY ./artifact-downloader/Cargo.toml ./Cargo.toml
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /artifact-downloader
COPY --from=planner /artifact-downloader/recipe.json /artifact-downloader/recipe.json

RUN cargo chef cook --release --recipe-path recipe.json
COPY ./artifact-downloader .

RUN cargo build --release

FROM alpine:3.21 AS runner

WORKDIR /app
RUN apk add ca-certificates libssl3 openssl

COPY --from=builder /artifact-downloader/target/release/artifact-downloader /app/executable
ENTRYPOINT ["/app/executable"]
