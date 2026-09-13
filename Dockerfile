# Use debian:bookworm-slim as the base image for both build and final stages
FROM debian:bookworm-slim AS base

# Install runtime dependencies (ca-certificates and curl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust in the build stage
FROM base AS builder

# Install necessary build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Build the project
RUN cargo build --release

# Broker stage: use the base runtime image
FROM base AS broker

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/danube-broker /usr/local/bin/danube-broker

# Copy the configuration file into the container
COPY config/danube_broker.yml /etc/danube_broker.yml

# Expose the ports: client, admin, Raft transport, Prometheus
EXPOSE 6650 50051 7650 9040

# Define entrypoint and default command
ENTRYPOINT ["/usr/local/bin/danube-broker"]
CMD ["--config-file", "/etc/danube_broker.yml"]

# CLI stage: use the base runtime image
FROM base AS cli

# Copy the compiled CLI binary from the builder stage
COPY --from=builder /app/target/release/danube-cli /usr/local/bin/danube-cli

# Define entrypoint
ENTRYPOINT ["/usr/local/bin/danube-cli"]

# Admin stage: consolidated admin binary with CLI and server modes
FROM base AS admin

# Copy the consolidated danube-admin binary from the builder stage
COPY --from=builder /app/target/release/danube-admin /usr/local/bin/danube-admin

# Expose the HTTP port (used in 'serve' mode)
EXPOSE 8080

# Define entrypoint
ENTRYPOINT ["/usr/local/bin/danube-admin"]

# Iceberg stage: lakehouse connector (sidecar)
FROM base AS iceberg

# Copy the danube-iceberg binary from the builder stage
COPY --from=builder /app/target/release/danube-iceberg /usr/local/bin/danube-iceberg

# Define entrypoint
ENTRYPOINT ["/usr/local/bin/danube-iceberg"]
CMD ["--config", "/etc/danube-iceberg-config.yaml"]