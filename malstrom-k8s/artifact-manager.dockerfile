FROM rust:1.85-alpine3.21 AS base
RUN apk add protoc musl-dev
RUN cargo install --locked cargo-chef && rm -rf ~/.cargo/registry/cache

FROM base AS planner
WORKDIR /artifact-manager
COPY ./artifact-manager/Cargo.toml ./Cargo.toml
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /artifact-manager
COPY --from=planner /artifact-manager/recipe.json /artifact-manager/recipe.json

RUN cargo chef cook --release --recipe-path recipe.json
COPY ./artifact-manager .

RUN cargo build --release

FROM alpine:3.21 AS runner

WORKDIR /app
RUN apk add ca-certificates libssl3

COPY --from=builder /artifact-manager/target/release/artifact-manager /app/executable
COPY ./artifact-manager/Rocket.toml ./Rocket.toml
ENTRYPOINT ["/app/executable"]
