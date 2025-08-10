FROM rust:1.85-alpine3.21 AS base
RUN apk add protoc musl-dev
RUN cargo install --locked cargo-chef && rm -rf ~/.cargo/registry/cache

FROM base AS planner
WORKDIR /app
COPY ./operator/Cargo.toml ./operator/Cargo.toml
COPY ./operator/crds ./operator/crds
WORKDIR /app/operator
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app/operator
COPY --from=planner /app/operator/recipe.json /app/operator/recipe.json
COPY --from=planner /app/operator/crds /app/operator/crds

RUN cargo chef cook --release --recipe-path recipe.json
COPY ./operator .

COPY ./runtime/proto /app/runtime/proto
RUN cargo build --release

FROM alpine:3.21 AS runner

WORKDIR /app
RUN apk add ca-certificates libssl3

COPY --from=builder /app/operator/target/release/malstrom-operator /app/executable
ENTRYPOINT ["/app/executable"]
