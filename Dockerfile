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

# Build the unified Danube binary (broker + embedded admin CLI/MCP + lakehouse runtime)
RUN cargo build --release --features danube-broker/lakehouse

# Runtime stage: single unified `danube` binary
FROM base

COPY --from=builder /app/target/release/danube /usr/local/bin/danube

# Expose client/admin (6650), Raft transport (7650), and Prometheus (9040)
EXPOSE 6650 7650 9040

ENTRYPOINT ["/usr/local/bin/danube"]
CMD ["up", "--data-dir", "/danube-data", "--broker-addr", "0.0.0.0:6650"]