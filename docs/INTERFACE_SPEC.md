# Interface Specification & API Contracts
## Project: RaftLite — Distributed Consensus & Replicated Key-Value Store
**Document Version:** 1.0.0  
**Target Release:** v1.0-GA  
**Status:** Approved for Implementation  
**Maintainer:** Lead Systems Architect  

---

### 1. Protocol Buffers (Protobuf v3) Specifications

#### 1.1 Consensus Service (`proto/raftlite/v1/raft.proto`)
The internal consensus protocol executed between cluster nodes.

```protobuf
syntax = "proto3";

package raftlite.v1;

option go_package = "github.com/raftlite/proto/raftlite/v1;raftlitev1";

// RaftService defines the internal consensus RPCs exchanged among RaftLite nodes.
service RaftService {
  // RequestVote is invoked by candidates to gather votes during elections (§5.2).
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);

  // AppendEntries is invoked by the leader to replicate log entries and send heartbeats (§5.3).
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
}

// LogEntryType categorizes the payload contained within a consensus log entry.
enum LogEntryType {
  ENTRY_TYPE_UNSPECIFIED = 0;
  ENTRY_TYPE_COMMAND = 1;       // Standard state machine mutation (Put, CAS, Delete)
  ENTRY_TYPE_NOOP = 2;          // Leader election assertion no-op
  ENTRY_TYPE_CONFIGURATION = 3; // Membership configuration metadata (reserved)
}

// LogEntry encapsulates a single replicated log entry.
message LogEntry {
  uint64 term = 1;           // Term when entry was proposed by leader
  uint64 index = 2;          // Monotonic 1-based log position
  LogEntryType type = 3;     // Payload categorization
  bytes data = 4;            // Serialized client command or metadata payload
}

// RequestVoteRequest arguments (§5.2).
message RequestVoteRequest {
  uint64 term = 1;           // Candidate's election term
  string candidate_id = 2;   // Candidate requesting vote
  uint64 last_log_index = 3; // Index of candidate's last log entry (§5.4.1)
  uint64 last_log_term = 4;  // Term of candidate's last log entry (§5.4.1)
}

// RequestVoteResponse results (§5.2).
message RequestVoteResponse {
  uint64 term = 1;           // Receiver's currentTerm, for candidate to update itself
  bool vote_granted = 2;     // True means candidate received vote
}

// AppendEntriesRequest arguments (§5.3).
message AppendEntriesRequest {
  uint64 term = 1;                  // Leader's current term
  string leader_id = 2;             // So follower can redirect clients
  uint64 prev_log_index = 3;        // Index of log entry immediately preceding new ones
  uint64 prev_log_term = 4;         // Term of prev_log_index entry
  repeated LogEntry entries = 5;    // Log entries to store (empty for heartbeat)
  uint64 leader_commit = 6;         // Leader's commitIndex
}

// AppendEntriesResponse results (§5.3 + Fast Log Backtracking extension).
message AppendEntriesResponse {
  uint64 term = 1;                  // Receiver's currentTerm, for leader to update itself
  bool success = 2;                 // True if follower contained entry matching prev_log_index/term
  uint64 match_index = 3;           // Highest index acknowledged by follower
  
  // Fast log conflict backtracking fields:
  uint64 conflict_term = 4;         // Term of conflicting entry in follower's log (0 if index gap)
  uint64 conflict_first_index = 5;  // First index for conflict_term in follower's log
}
```

---

#### 1.2 Key-Value & Client Service (`proto/raftlite/v1/kv.proto`)
The public client API exposed to external applications and administrative utilities.

```protobuf
syntax = "proto3";

package raftlite.v1;

option go_package = "github.com/raftlite/proto/raftlite/v1;raftlitev1";

// KVService exposes linearizable key-value storage operations to clients.
service KVService {
  rpc ClientPut(ClientPutRequest) returns (ClientPutResponse);
  rpc ClientGet(ClientGetRequest) returns (ClientGetResponse);
  rpc ClientCAS(ClientCASRequest) returns (ClientCASResponse);
  rpc ClientDelete(ClientDeleteRequest) returns (ClientDeleteResponse);
  rpc ClusterStatus(ClusterStatusRequest) returns (ClusterStatusResponse);
}

// ClientPutRequest stores or updates a key.
message ClientPutRequest {
  string client_id = 1;      // Unique client identifier for deduplication
  uint64 sequence_num = 2;   // Monotonic client sequence number
  string key = 3;            // Target key
  string value = 4;          // Value payload to write
}

message ClientPutResponse {
  bool success = 1;          // True if successfully committed and applied
  string error = 2;          // Error message if unsuccessful
  string leader_hint = 3;    // Current known leader address if contacted node is follower
}

// ClientGetRequest retrieves a key.
message ClientGetRequest {
  string key = 1;            // Target key
  bool linearizable = 2;     // If true, asserts quorum lease/read-index before returning
}

message ClientGetResponse {
  bool found = 1;            // True if key exists
  string value = 2;          // Retrieved string value
  string error = 3;          // Error message if unsuccessful
  string leader_hint = 4;    // Current known leader address if contacted node is follower
}

// ClientCASRequest performs an atomic Compare-And-Swap operation.
message ClientCASRequest {
  string client_id = 1;
  uint64 sequence_num = 2;
  string key = 3;
  string expected_value = 4; // Expected existing value (empty string if expected non-existent)
  string new_value = 5;      // Value to write if condition matches
}

message ClientCASResponse {
  bool success = 1;          // True if consensus log replication succeeded
  bool swapped = 2;          // True if condition matched and swap executed
  string current_value = 3;  // Existing value if swap failed
  string error = 4;
  string leader_hint = 5;
}

// ClientDeleteRequest removes a key.
message ClientDeleteRequest {
  string client_id = 1;
  uint64 sequence_num = 2;
  string key = 3;
}

message ClientDeleteResponse {
  bool success = 1;
  bool found = 2;
  string error = 3;
  string leader_hint = 4;
}

// ClusterStatus inspection messages.
message ClusterStatusRequest {}

message PeerStatus {
  string peer_id = 1;
  string address = 2;
  uint64 match_index = 3;
  uint64 next_index = 4;
  bool reachable = 5;
}

message ClusterStatusResponse {
  string node_id = 1;
  string role = 2;           // FOLLOWER, CANDIDATE, LEADER
  uint64 current_term = 3;
  uint64 commit_index = 4;
  uint64 last_applied = 5;
  string leader_id = 6;
  repeated PeerStatus peers = 7;
}
```

---

### 2. gRPC Error Handling & Status Codes

| gRPC Status Code | When Returned | Client Action / Mitigation |
| :--- | :--- | :--- |
| `codes.OK` | Operation successfully committed, applied, and verified. | Complete request. |
| `codes.Unavailable` | Contacted node is a **Follower** or **Candidate**; or node is in partition. | Inspect `leader_hint` field in response. If set, redirect connection to leader; otherwise, retry with backoff. |
| `codes.DeadlineExceeded` | Request failed to achieve quorum replication within context deadline. | Re-send request with identical `client_id` and `sequence_num` (deduplication guarantees exactly-once). |
| `codes.Unauthenticated` | Missing or invalid cluster token header (`x-raftlite-cluster-token`). | Verify cluster authentication configuration. |
| `codes.InvalidArgument` | Empty key, negative sequence number, or invalid request payload. | Fix request parameters; do not retry. |

---

### 3. Server Daemon CLI Specification: `raftd`

The `raftd` binary is the core server daemon. It initializes storage, starts the consensus state machine, connects to peers, and serves gRPC requests.

#### 3.1 Command-Line Flags & Environment Variables

```
Usage of raftd:
  --config string
        Path to YAML configuration file (optional; flags override config).
  --node-id string
        Unique identifier for this node (env: RAFTLITE_NODE_ID). Required.
  --listen-addr string
        Internal consensus gRPC bind address (env: RAFTLITE_LISTEN_ADDR). (default "0.0.0.0:9091")
  --client-addr string
        External client KV gRPC bind address (env: RAFTLITE_CLIENT_ADDR). (default "0.0.0.0:8081")
  --peers string
        Comma-separated list of peer specifications: "node2=host2:port,node3=host3:port"
        (env: RAFTLITE_PEERS).
  --data-dir string
        Local directory path for binary WAL and state persistence (env: RAFTLITE_DATA_DIR).
        (default "./data")
  --election-min-ms int
        Minimum randomized election timeout in milliseconds. (default 150)
  --election-max-ms int
        Maximum randomized election timeout in milliseconds. (default 300)
  --heartbeat-ms int
        Leader heartbeat interval in milliseconds. (default 50)
  --sync
        Enforce synchronous fsync() on every log append. (env: RAFTLITE_SYNC). (default true)
  --cluster-token string
        Shared cluster authorization token for isolation (env: RAFTLITE_CLUSTER_TOKEN).
  --log-level string
        Logging verbosity: "debug", "info", "warn", "error". (default "info")
```

#### 3.2 Signal Handling & Lifecycle
- `SIGINT` / `SIGTERM`: Triggers graceful shutdown. The server stops accepting new client requests, flushes pending state machine operations, syncs the WAL file, closes peer connections, and exits with code 0.
- `SIGKILL`: Abrupt process termination. Tested under chaos suites to verify WAL crash recovery and zero uncommitted data corruption.

---

### 4. Client & Benchmark CLI Specification: `raftctl`

The `raftctl` utility provides key-value operations, cluster health queries, and a high-concurrency benchmarking tool.

#### 4.1 Global Flags
- `--endpoints string`: Comma-separated list of cluster client endpoints (e.g., `127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083`).
- `--timeout duration`: Maximum request timeout (default `3s`).
- `--token string`: Cluster authentication token.
- `--json`: Format output as machine-readable JSON.

#### 4.2 Subcommands & Syntax

##### 1. Write Key (`put`)
```bash
raftctl put <key> <value> [flags]
# Example:
raftctl --endpoints=localhost:8081 put user:1001 '{"name":"Alice","role":"admin"}'
# Output:
# OK (committed at term 2, index 48)
```

##### 2. Read Key (`get`)
```bash
raftctl get <key> [flags]
# Example:
raftctl --endpoints=localhost:8081 get user:1001
# Output:
# {"name":"Alice","role":"admin"}
```

##### 3. Compare-And-Swap (`cas`)
```bash
raftctl cas <key> <expected_value> <new_value> [flags]
# Example:
raftctl --endpoints=localhost:8081 cas user:1001:status "OFFLINE" "ONLINE"
# Output:
# SWAPPED (committed at term 2, index 49)
```

##### 4. Delete Key (`delete`)
```bash
raftctl delete <key> [flags]
# Example:
raftctl --endpoints=localhost:8081 delete user:1001
# Output:
# DELETED (committed at term 2, index 50)
```

##### 5. Cluster Inspection (`status`)
```bash
raftctl status [flags]
# Output:
# Node ID:      node-1
# Role:         LEADER
# Current Term: 2
# Commit Index: 50
# Last Applied: 50
# Peers (2):
#   - node-2 (127.0.0.1:9092): matchIndex=50, nextIndex=51 [HEALTHY]
#   - node-3 (127.0.0.1:9093): matchIndex=50, nextIndex=51 [HEALTHY]
```

##### 6. Benchmark Load Generator (`bench`)
```bash
raftctl bench [flags]
  --clients int          Number of concurrent client workers (default 16)
  --duration duration    Total benchmark test duration (default 30s)
  --key-size int         Key size in bytes (default 16)
  --val-size int         Value payload size in bytes (default 128)
  --write-ratio float    Ratio of writes vs reads [0.0 to 1.0] (default 1.0)
```

**Benchmark Output Format:**
```
==================== RaftLite Benchmark Results ====================
Duration:             30.002s
Concurrent Clients:   32
Total Operations:     124,892
Successful Ops:       124,892 (100.00%)
Failed Ops:           0 (0.00%)
Throughput:           4,162.8 ops/sec
Data Transferred:     16.38 MB

Latency Distribution:
  P50 (Median):       3.24 ms
  P90:                5.12 ms
  P99:                8.94 ms
  Max Latency:        21.40 ms
====================================================================
```

---

### 5. Multi-Node Docker Compose Cluster Specifications

#### 5.1 3-Node Cluster (`deploy/docker-compose.3node.yml`)
```yaml
version: '3.8'

networks:
  raftlite-net:
    driver: bridge

services:
  node-1:
    build:
      context: ..
      dockerfile: deploy/Dockerfile
    container_name: raftlite-node-1
    command: >
      /usr/local/bin/raftd
      --node-id=node-1
      --listen-addr=0.0.0.0:9091
      --client-addr=0.0.0.0:8081
      --peers=node-2=node-2:9092,node-3=node-3:9093
      --data-dir=/var/lib/raftlite/data
      --sync=true
    ports:
      - "8081:8081"
      - "9091:9091"
    volumes:
      - node1-data:/var/lib/raftlite/data
    networks:
      - raftlite-net

  node-2:
    build:
      context: ..
      dockerfile: deploy/Dockerfile
    container_name: raftlite-node-2
    command: >
      /usr/local/bin/raftd
      --node-id=node-2
      --listen-addr=0.0.0.0:9092
      --client-addr=0.0.0.0:8082
      --peers=node-1=node-1:9091,node-3=node-3:9093
      --data-dir=/var/lib/raftlite/data
      --sync=true
    ports:
      - "8082:8082"
      - "9092:9092"
    volumes:
      - node2-data:/var/lib/raftlite/data
    networks:
      - raftlite-net

  node-3:
    build:
      context: ..
      dockerfile: deploy/Dockerfile
    container_name: raftlite-node-3
    command: >
      /usr/local/bin/raftd
      --node-id=node-3
      --listen-addr=0.0.0.0:9093
      --client-addr=0.0.0.0:8083
      --peers=node-1=node-1:9091,node-2=node-2:9092
      --data-dir=/var/lib/raftlite/data
      --sync=true
    ports:
      - "8083:8083"
      - "9093:9093"
    volumes:
      - node3-data:/var/lib/raftlite/data
    networks:
      - raftlite-net

volumes:
  node1-data:
  node2-data:
  node3-data:
```

#### 5.2 5-Node Cluster (`deploy/docker-compose.5node.yml`)
The 5-node cluster expands the deployment to tolerate 2 simultaneous node crashes ($F=2$, Quorum=3). Nodes 1 through 5 are assigned client ports `8081..8085` and consensus ports `9091..9095`, each backed by dedicated isolated Docker named volumes (`node1-data` through `node5-data`).
