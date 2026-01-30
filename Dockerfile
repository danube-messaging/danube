# Use debian:bullseye-slim as the base image for both build and final stages
FROM debian:bullseye-slim AS base

# Install Rust in the build stage
FROM base AS builder

# Install necessary dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    protobuf-compiler

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Build the project
RUN cargo build --release

# Broker stage: use the same base image as the build stage
FROM base AS broker

# Install protobuf-compiler in the final image as well
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/danube-broker /usr/local/bin/danube-broker

# Copy the configuration file into the container (adjust the path if needed)
COPY config/danube_broker.yml /etc/danube_broker.yml

# Expose the ports your broker listens on
EXPOSE 6650 6651

# Define entrypoint and default command
ENTRYPOINT ["/usr/local/bin/danube-broker"]
CMD ["--config-file", "/etc/danube_broker.yml", "--broker-addr", "0.0.0.0:6650", "--advertised-addr", "0.0.0.0:6650"]

# CLI stage: use the same base image as the build stage
FROM base AS cli

# Install protobuf-compiler and curl for testing
RUN apt-get update && apt-get install -y protobuf-compiler curl

# Copy the compiled CLI binary from the builder stage
COPY --from=builder /app/target/release/danube-cli /usr/local/bin/danube-cli

# No default entrypoint - users can call the binary directly
ENTRYPOINT []

# Admin stage: consolidated admin binary with CLI and server modes
FROM base AS admin

# Copy the consolidated danube-admin binary from the builder stage
COPY --from=builder /app/target/release/danube-admin /usr/local/bin/danube-admin

# Expose the HTTP port (used in 'serve' mode)
EXPOSE 8080

# Define entrypoint
ENTRYPOINT ["/usr/local/bin/danube-admin"]