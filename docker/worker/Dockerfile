FROM rust:1.91-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin explorer-worker

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/explorer-worker /usr/local/bin/explorer-worker
ENTRYPOINT ["explorer-worker"]
