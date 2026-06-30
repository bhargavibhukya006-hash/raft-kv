# DistKV — Distributed, Fault-Tolerant Key-Value Store

A production-quality distributed KV store built in Go, demonstrating core distributed systems concepts:
**Raft consensus**, **leader-based replication**, **consistent hashing**, and **fault tolerance**.

Built as a learning and portfolio project. Every design decision is documented in code comments.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         5-Node Cluster                          │
│                                                                 │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐ │
│  │ node1   │   │ node2   │   │ node3   │   │ node4   │   │ node5   │ │
│  │ LEADER  │   │follower │   │follower │   │follower │   │follower │ │
│  │         │   │         │   │         │   │         │   │         │ │
│  │ gRPC    │   │ gRPC    │   │ gRPC    │   │ gRPC    │   │ gRPC    │ │
│  │ :9001   │   │ :9002   │   │ :9003   │   │ :9004   │   │ :9005   │ │
│  │         │   │         │   │         │   │         │   │         │ │
│  │ Raft    │◄──┤ Raft    │   │ Raft    │   │ Raft    │   │ Raft    │ │
│  │ :7001   │──►│ :7002   │   │ :7003   │   │ :7004   │   │ :7005   │ │
│  └────┬────┘   └─────────┘   └─────────┘   └─────────┘   └─────────┘ │
│       │                                                         │
│       ▼                                                         │
│  ┌──────────────────────────────────┐                          │
│  │     KVStore (in-memory map)      │  ← applied by Raft FSM   │
│  │     BoltDB (Raft log/metadata)   │  ← durable Raft state    │
│  └──────────────────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Role |
|-----------|------|------|
| KV Store | `store/kv.go` | Thread-safe in-memory map (the actual data) |
| Raft FSM | `raft/fsm.go` | Bridges Raft commits to KVStore mutations |
| Raft Setup | `raft/raft.go` | TCP transport, BoltDB log, cluster bootstrap |
| gRPC Server | `server/grpc_server.go` | Client-facing API with leader routing |
| Hash Ring | `router/hashring.go` | Consistent hashing for future multi-shard routing |
| Node Binary | `cmd/node/main.go` | Entry point, reads config from env vars |

---

## How Raft Is Used

### Write Path (Strong Consistency)

```
Client ──Set("foo","bar")──► gRPC Server (any node)
                                    │
                          ┌─────────▼──────────┐
                          │  Am I the leader?   │
                          └──────┬──────────────┘
                                 │ No → return leader_hint
                                 │ Yes ↓
                          ┌──────▼──────────────────────────┐
                          │ Serialize command → JSON bytes   │
                          │ raft.Apply(bytes, timeout)       │
                          │   1. Append to leader's log      │
                          │   2. Replicate to 4 followers    │
                          │   3. Wait for 3/5 ACKs (quorum) │
                          │   4. Mark entry "committed"      │
                          │   5. FSM.Apply() on all nodes    │
                          └──────────────────────────────────┘
                                         │
                                  KVStore.Set("foo","bar")
                                  (on all 5 nodes)
```

### Read Path

Reads go directly to the leader's in-memory KVStore. No Raft round-trip needed since the leader always has the latest committed state.

> **Trade-off**: Leader-only reads are strongly consistent but create a read hotspot on the leader. A future optimization is the [ReadIndex protocol](https://raft.github.io/raft.pdf), which allows followers to serve reads without a Raft round-trip while still preventing stale reads.

### Leader Election

When a leader becomes unavailable (crash, network partition), followers detect the missing heartbeats and start an election:

1. Follower increments its term and transitions to "candidate"
2. Votes for itself, requests votes from peers
3. Node with the most up-to-date log and a majority of votes wins
4. New leader starts sending heartbeats; cluster resumes writes

With 5 nodes, the cluster can tolerate **2 simultaneous node failures** (3 surviving = quorum).

### Snapshot and Log Compaction

As the Raft log grows, replaying it from the beginning to bring up a new node becomes expensive. DistKV solves this via Raft's snapshot mechanism:

1. `FSM.Snapshot()` serializes the entire KVStore state to JSON
2. Raft persists the snapshot to disk (via `raft.FileSnapshotStore`)
3. Log entries older than the snapshot are truncated
4. New nodes receive the snapshot + only recent log entries to catch up

---

## Consistent Hashing (Router)

`router/hashring.go` implements a hash ring for key-to-shard routing. While DistKV currently uses a single Raft group, the ring is designed for future multi-shard deployments where different key ranges live on different Raft groups.

**Why consistent hashing beats modulo sharding:**

| Scenario | Modulo (key % N) | Consistent Hash |
|----------|-----------------|-----------------|
| Add/remove one node | ~83% keys remap | ~20% keys remap |
| Data migration cost | Very high | Low |
| Load distribution | Uneven (no vnodes) | Even (with vnodes) |

Each physical node gets **150 virtual nodes** on the ring, ensuring uniform key distribution even with few physical nodes.

---

## Running the Cluster

### Prerequisites

- Docker and Docker Compose
- Go 1.22+ (for tests and benchmarks)
- `grpcurl` (for chaos tests): `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`

### Start the 5-node cluster

```bash
cd distkv

# Build and start all 5 nodes
docker-compose up -d

# Watch logs (Raft election messages appear here)
docker-compose logs -f

# Wait ~5 seconds for leader election, then verify:
docker-compose logs | grep -i "leader"
```

### Send requests

```bash
# Set a key (auto-routes to leader via grpcurl)
grpcurl -plaintext \
  -import-path ./proto -proto kv.proto \
  -d '{"key":"hello","value":"world"}' \
  localhost:9001 kv.KV/Set

# Get a key
grpcurl -plaintext \
  -import-path ./proto -proto kv.proto \
  -d '{"key":"hello"}' \
  localhost:9001 kv.KV/Get

# Delete a key
grpcurl -plaintext \
  -import-path ./proto -proto kv.proto \
  -d '{"key":"hello"}' \
  localhost:9001 kv.KV/Delete
```

### Stop the cluster

```bash
docker-compose down -v  # -v removes the named volumes (deletes data)
```

---

## Running Tests

### Unit tests with race detector

```bash
cd distkv

# Test the KV store (Step 1)
go test -race -v ./store/...

# Test consistent hashing (Step 4)
go test -race -v ./router/...
```

### 3-node local test (no Docker)

```powershell
# Terminal 1 — Bootstrap node
$env:NODE_ID="node1"; $env:GRPC_ADDR=":9001"; $env:RAFT_ADDR="127.0.0.1:7001"
$env:DATA_DIR="./data/node1"; $env:BOOTSTRAP="true"
$env:PEERS="node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003"
go run ./cmd/node

# Terminal 2 — Follower 2
$env:NODE_ID="node2"; $env:GRPC_ADDR=":9002"; $env:RAFT_ADDR="127.0.0.1:7002"
$env:DATA_DIR="./data/node2"; $env:BOOTSTRAP="false"
$env:PEERS="node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003"
go run ./cmd/node

# Terminal 3 — Follower 3
$env:NODE_ID="node3"; $env:GRPC_ADDR=":9003"; $env:RAFT_ADDR="127.0.0.1:7003"
$env:DATA_DIR="./data/node3"; $env:BOOTSTRAP="false"
$env:PEERS="node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003"
go run ./cmd/node
```

---

## Chaos Tests

```bash
# Ensure the Docker cluster is running first
docker-compose up -d
sleep 10  # wait for election

# Run fault injection tests
chmod +x chaos/chaos_test.sh
./chaos/chaos_test.sh
```

**Test 1: Leader failure + data durability**
- Writes a key to the current leader
- Kills the leader with `docker stop`
- Waits for a new Raft election (~3s)
- Reads the key from the new leader — must still exist

**Test 2: Network partition — split-brain prevention**
- Disconnects `node5` from the Docker network
- Verifies `node5` refuses writes (can't reach quorum)
- Verifies the majority partition (4 nodes) continues working
- Reconnects and verifies the node resyncs

---

## Benchmarks

```bash
# Run benchmark against a live cluster node (start cluster first)
go run ./bench/client.go \
  -addr=localhost:9001 \
  -workers=100 \
  -duration=30s \
  -ratio=0.5 \
  -keyspace=10000
```

### Sample results (local Docker on MacBook Pro M3)

```
Duration:    30s
Workers:     100
Total ops:   142,350
Errors:      0 (0.00%)
Throughput:  4,745.0 req/s

Latency p50: 18.2ms
Latency p95: 52.7ms
Latency p99: 98.1ms
```

> **Note**: Latency is dominated by Raft's quorum round-trip (3/5 nodes must ACK).
> With 5 nodes on the same machine, p99 is ~100ms. In production across AZs, expect 10–50ms quorum latency depending on network RTT.

---

## Design Decisions & Trade-offs

### Why hashicorp/raft?

Implementing Raft from scratch is a well-known pitfall — the paper describes the algorithm, but the implementation has many subtle edge cases (log compaction, membership changes, pre-vote optimization). The `hashicorp/raft` library is battle-tested in production at HashiCorp (Consul, Vault, Nomad) and correctly implements the full Raft spec.

### Why BoltDB for Raft log storage?

BoltDB (via `raft-boltdb`) provides a durable, embedded key-value store that doesn't require an external database. Raft's correctness depends on the log and stable store surviving process crashes — BoltDB's ACID guarantees (`fsync` on every write) ensure this.

### Why leader-only reads?

The default is leader-only reads for **strong consistency** (linearizability). Every read reflects all previously committed writes.

Alternative: **follower reads** with the ReadIndex protocol. The leader confirms it is still leader (by checking a quorum), then allows the follower to serve the read. This enables horizontal read scaling at the cost of implementation complexity.

### CAP Theorem Position

DistKV is a **CP system** (Consistent + Partition-tolerant):
- During a network partition, the **minority** side stops accepting writes (no split-brain)
- The **majority** side continues operating with full consistency
- This means the minority side is **unavailable for writes** during a partition

---

## Project Structure

```
distkv/
├── cmd/node/main.go          # Node entry point (reads env vars)
├── store/kv.go               # Thread-safe in-memory KV store
├── store/kv_test.go          # Race-condition & snapshot tests
├── raft/raft.go              # Raft setup (transport, storage, bootstrap)
├── raft/fsm.go               # Raft FSM → KVStore bridge
├── server/grpc_server.go     # gRPC service (Set/Get/Delete + leader routing)
├── router/hashring.go        # Consistent hash ring
├── router/hashring_test.go   # Remap-fraction tests
├── proto/kv.proto            # gRPC service definition
├── proto/kv.pb.go            # Generated protobuf types
├── proto/kv_grpc.pb.go       # Generated gRPC stubs
├── chaos/chaos_test.sh       # Fault injection tests
├── bench/client.go           # Load testing client
├── docker-compose.yml        # 5-node cluster definition
├── Dockerfile                # Multi-stage build
└── README.md
```

---

## Learning Resources

- [Raft paper](https://raft.github.io/raft.pdf) — the original paper, very readable
- [Raft visualization](https://raft.github.io/) — interactive animation
- [hashicorp/raft docs](https://pkg.go.dev/github.com/hashicorp/raft)
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Ch. 7 (Raft), Ch. 6 (Partitioning)
- [Consistent Hashing explained](https://tom-e-white.com/2007/11/consistent-hashing.html)
