# RaftLite — Distributed Consensus & Replicated Key-Value Store

[![Go Version](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat&logo=go)](https://golang.org)
[![Consensus Protocol](https://img.shields.io/badge/Consensus-Raft%20(CFT)-blueviolet.svg)](https://raft.github.io)
[![Storage Engine](https://img.shields.io/badge/Storage-Binary%20WAL%20(CRC32)-orange.svg)]()
[![Race Detector](https://img.shields.io/badge/Race%20Detector-100%25%20Clean-success.svg)]()
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**RaftLite** is a high-performance, crash-fault-tolerant (CFT) distributed consensus engine and replicated Key-Value store implemented in **Go 1.22** using **gRPC** and **Protocol Buffers**.

Engineered for extreme reliability, concurrency safety, and determinism, RaftLite implements the complete Raft consensus specification (Ongaro & Ousterhout) enhanced with production-grade durability: an append-only binary Write-Ahead Log (WAL) with CRC32-IEEE framing, torn write auto-recovery, accelerated term-based log conflict backtracking, linearizable client request deduplication, and an in-process network chaos emulation interceptor (`netem`).

---

## Key Capabilities & Core Innovations

1. **Raft Consensus State Machine:**
   - Formal implementation of **Leader**, **Follower**, and **Candidate** roles.
   - Randomized election timeouts ($150\text{ms} - 300\text{ms}$) with leak-free timer drainage.
   - Frequent leader heartbeats ($50\text{ms}$) to suppress spurious elections.
   - Strict adherence to the **Election Restriction Rule (§5.4.1)**: candidates with stale logs cannot be elected.
2. **Fast Log Conflict Backtracking:**
   - Term-based accelerated backtracking: follower returns `conflictTerm` and `conflictFirstIndex` upon log mismatch, allowing the leader to bypass entire conflicting terms in a single RPC round-trip rather than decrementing `nextIndex` by 1 per entry.
3. **Majority CommitIndex Advancement:**
   - Leader dynamically evaluates peer `matchIndex` medians, strictly enforcing **Ongaro's Rule (§5.4.2)**: entries from prior terms are *never* committed by replica count alone, preventing historic log overwrite bugs.
4. **Binary Write-Ahead Log (WAL) with CRC32 Framing:**
   - Append-only binary framing: `[CRC32: 4B | Term: 8B | Index: 8B | Type: 1B | CommandLen: 4B | CommandBytes: NB]`.
   - Synchronous `fsync()` guarantees before acknowledging RPCs.
   - Automated crash scanner with torn-write truncation at corrupted frame boundaries.
5. **Linearizable Replicated State Machine & Client Deduplication:**
   - In-memory key-value engine supporting atomic `Get`, `Put`, `Delete`, and `CAS` (Compare-And-Swap) operations over an asynchronous `applyCh`.
   - Client session deduplication table (`clientId` + `sequenceNum`) caching results to guarantee strictly *exactly-once* execution across retries and leader failovers.
6. **Network Chaos & Fault-Injection Harness (`netem`):**
   - In-process gRPC client/server interceptors simulating asymmetric network partitions, arbitrary packet drop rates, and configurable latency/jitter without requiring root OS access.
7. **Production Tooling & Cluster Orchestration:**
   - Main cluster server daemon: `raftd`.
   - Administrative operator and benchmarking utility: `raftctl`.
   - Ready-to-run 3-node and 5-node Docker Compose topologies.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Client CLI (raftctl) /    |
                      |   Application SDK (gRPC)    |
                      +--------------+--------------+
                                     |
                                     v
+------------------------------------+------------------------------------+
|  RaftLite Node (raftd)                                                  |
|                                                                         |
|   +-----------------------+             +---------------------------+   |
|   | KVService (Port 8081) |             | RaftService (Port 9091)   |   |
|   | Get, Put, CAS, Delete |             | AppendEntries, RequestVote|   |
|   +-----------+-----------+             +-------------+-------------+   |
|               |                                       |                 |
|               v                                       v                 |
|   +-------------------------------------------------------------+       |
|   |                   Raft Consensus Core                       |       |
|   |      (Role State Machine, Election Timers, Heartbeats)      |       |
|   +-------------+-------------------------------+---------------+       |
|                 |                               |                       |
|                 v (applyCh)                     v (fsync)               |
|   +---------------------------+   +---------------------------------+   |
|   | Replicated KV State       |   | Binary Write-Ahead Log (WAL)    |   |
|   | - In-Memory Store         |   | - CRC32 IEEE Framing            |   |
|   | - Client Dedup Session    |   | - Torn Write Truncation Scanner |   |
|   +---------------------------+   +---------------------------------+   |
+-------------------------------------------------------------------------+
```

---

## Pre-Build Engineering Documentation

Comprehensive engineering specifications are located in the [`docs/`](docs/) directory:

| Document | Purpose & Contents | Link |
| :--- | :--- | :--- |
| **Product Requirements Document (PRD)** | Problem statement, target personas, product vision, feature requirements matrix, end-to-end request flows, and success metrics. | [docs/PRD.md](docs/PRD.md) |
| **System Architecture Specification** | Tech stack justification, folder structure, binary WAL framing, fast backtracking algorithm, state machine deduplication, and Mermaid diagrams. | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| **Security & Threat Model** | CFT vs BFT assumptions, transport security (mTLS, cluster tokens), storage integrity (CRC32), edge cases (split-brain, stale reads), and failure recovery playbook. | [docs/SECURITY.md](docs/SECURITY.md) |
| **Interface Specification** | Complete Protocol Buffers (`raft.proto`, `kv.proto`), gRPC status codes, `raftd` daemon flags, `raftctl` CLI commands, and Docker Compose files. | [docs/INTERFACE_SPEC.md](docs/INTERFACE_SPEC.md) |
| **Feature Tickets & Roadmap** | 13 atomic, sequential implementation tickets (TICKET-RL-001 through TICKET-RL-013) with precise AI prompts and verifiable acceptance criteria. | [docs/FEATURE_TICKETS.md](docs/FEATURE_TICKETS.md) |

---

## Quickstart & Deployment

### 1. Prerequisites
- **Go:** `1.22` or later
- **Protocol Buffers Compiler:** `protoc` (v3.20+) and `protoc-gen-go`, `protoc-gen-go-grpc`
- **Docker & Docker Compose:** for containerized multi-node testing

### 2. Building from Source
```bash
# Clone repository
git clone https://github.com/raftlite/raftlite.git
cd raftlite

# Compile Protocol Buffers
make proto

# Build server daemon (raftd) and CLI (raftctl)
make build
```
Binaries will be placed in `bin/raftd` and `bin/raftctl`.

### 3. Launching a 3-Node Cluster via Docker Compose
To start an isolated 3-node RaftLite cluster with persistent storage volumes:
```bash
docker compose -f deploy/docker-compose.3node.yml up -d
```

Verify cluster status using `raftctl`:
```bash
./bin/raftctl --endpoints=localhost:8081 status
```
Output:
```text
Node ID:      node-1
Role:         LEADER
Current Term: 1
Commit Index: 1
Last Applied: 1
Peers (2):
  - node-2 (node-2:9092): matchIndex=1, nextIndex=2 [HEALTHY]
  - node-3 (node-3:9093): matchIndex=1, nextIndex=2 [HEALTHY]
```

### 4. Executing Key-Value Operations
```bash
# Store a key-value pair
./bin/raftctl --endpoints=localhost:8081 put alpha "42"

# Read the value back
./bin/raftctl --endpoints=localhost:8081 get alpha

# Execute an atomic Compare-And-Swap (CAS)
./bin/raftctl --endpoints=localhost:8081 cas alpha "42" "100"

# Delete the key
./bin/raftctl --endpoints=localhost:8081 delete alpha
```

### 5. Running the High-Throughput Benchmarker
Benchmark the cluster under high concurrent load:
```bash
./bin/raftctl bench \
  --endpoints=localhost:8081,localhost:8082,localhost:8083 \
  --clients=32 \
  --duration=30s \
  --write-ratio=1.0
```

---

## Testing & Quality Verification

RaftLite enforces strict zero-tolerance policies for race conditions and data loss:

```bash
# Execute full unit and integration test suite under the Go race detector
go test -v -race -count=1 ./...

# Run 50 continuous rounds of network chaos and leader crash recovery
go test -v -race -run TestClusterChaos ./tests/chaos/...
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
