# ---- Stage 1: Build ----
# Use the official Go image for a reproducible build environment.
# golang:1.22-alpine is smaller than the full Debian image.
FROM golang:1.22-alpine AS builder

# Install git (needed for go mod download to fetch from VCS)
RUN apk add --no-cache git

WORKDIR /app

# Copy go.mod first (and go.sum if it exists) so Docker can cache the dependency layer.
# This layer is only invalidated when dependencies change, not source code.
COPY go.mod go.sum* ./
RUN go mod download

# Copy all source code
COPY . .

# Run go mod tidy to generate go.sum inside the container
RUN go mod tidy

# Build the node binary.
# CGO_ENABLED=0: static binary, no C dependencies — works in scratch/distroless.
# -ldflags "-s -w": strip debug symbols to reduce binary size.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /app/distkv-node \
    ./cmd/node

# ---- Stage 2: Runtime ----
# debian:bookworm-slim is a minimal Debian image (~80MB).
# Much smaller than golang:1.22 (~1GB) but includes libc for debugging tools.
FROM debian:bookworm-slim

# Install ca-certificates for HTTPS calls and useful debugging tools
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd -r -s /bin/false distkv

# Create data directory for Raft log/snapshot storage
RUN mkdir -p /data && chown distkv:distkv /data

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/distkv-node .

# Switch to non-root user
USER distkv

# Default ports:
# 9000-9004: gRPC (one per node)
# 7000-7004: Raft TCP (one per node)
EXPOSE 9000 7000

ENTRYPOINT ["./distkv-node"]
