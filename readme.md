# go-redis-raft

A distributed key-value store built from scratch in Go, using the **Raft consensus algorithm** for replication and fault tolerance. No external Raft libraries — the full leader election, log replication, and cluster membership protocol is implemented by hand.

## Architecture

```
┌──────────────────────────────────────────────┐
│          REST API Gateway (service)           │
│  POST /redis/set   GET /redis/get            │
│  POST /redis/del   GET  /cluster/status       │
└──────────────────────┬───────────────────────┘
                       │  discovers leader, forwards writes /raft/command
                       | 
                       ▼
┌──────────────────────────────────────────────┐
│          Raft Consensus Nodes                 │
│                                              │
│   node1 (leader)   node2       node3         │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│   │ RaftNode │  │ RaftNode │  │ RaftNode │  │
│   │ KV store │  │ KV store │  │ KV store │  │
│   │ WAL log  │  │ WAL log  │  │ WAL log  │  │
│   └──────────┘  └──────────┘  └──────────┘  │
│        │  replicates log entries             │
│        └──────────────┬──────────────────────┘
│                       │ all nodes apply committed entries
└──────────────────────────────────────────────┘
```

**Three components:**

| Component | Binary flag | Default port | Role |
|---|---|---|---|
| Raft node | `redis` | 5000 | Consensus, log replication, KV state machine |
| REST gateway | `service` | 8000 | Forwards client requests to the leader |

## Features

- **Leader election** — randomised timeouts prevent split votes; single-node cluster elects itself immediately
- **Log replication** — leader appends, replicates to quorum, advances `commitIndex`; write returns only after commit
- **Persistent WAL** — binary write-ahead log survives restarts
- **Health-check worker** — each node pings peers every 200 ms; detects dead leader in ~400 ms and triggers re-election without waiting for the election timer
- **Dynamic membership** — new nodes join at runtime via `POST /raft/join`; leader broadcasts membership to existing peers and immediately replicates the full log
- **TTL expiry** — keys auto-expire; a background worker sweeps every 500 ms

## Quick start

```bash
# Build and start a 3-node cluster + REST gateway
docker compose up --build

# Set a key (TTL optional, in ms)
curl -X POST http://localhost:8001/redis/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"world","expire_ms":60000}'

# Get a key
curl -X POST http://localhost:8001/redis/get \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello"}'

# Delete a key
curl -X POST http://localhost:8001/redis/del \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello"}'

# Cluster status
curl http://localhost:8001/cluster/status
```

## Responses

**SET / DEL**
```json
{"node_id": "node1", "index": 4}
```
`node_id` is the Raft leader that committed the entry; `index` is the log index.

**GET**
```json
{"node_id": "node1", "value": "world", "found": true}
```

## Environment variables

### Raft node (`redis` command)

| Variable | Default | Description |
|---|---|---|
| `NODE` | `node1` | Unique node ID |
| `PORT` | `5000` | HTTP listen port |
| `PEERS` | _(empty)_ | Comma-separated peers: `node1=http://node1:5000,node2=http://node2:5000` |
| `LOGFILE` | `.raft-<NODE>.log` | WAL log file path |
| `STATEFILE` | `.raft-<NODE>.state` | Persistent state file (term, votedFor) |
| `JOIN_ADDR` | _(empty)_ | Set to any existing node URL to join a running cluster |
| `SELF_ADDR` | `http://<NODE>:<PORT>` | This node's reachable URL (used in join requests) |

### REST gateway (`service` command)

| Variable | Default | Description |
|---|---|---|
| `NODE` | `service` | Service ID |
| `PORT` | `8000` | HTTP listen port |
| `PEERS` | _(empty)_ | Raft node addresses (same format as above) |
| `DATAFILE` | `.data-<NODE>.json` | Local data cache |

## HTTP endpoints

### Raft nodes (port 5000)

| Method | Path | Description |
|---|---|---|
| `POST` | `/raft/vote` | RequestVote RPC (internal) |
| `POST` | `/raft/append` | AppendEntries RPC (internal) |
| `POST` | `/raft/command` | Client write — leader appends, replicates, waits for commit |
| `POST` | `/raft/get` | Client read from local KV store |
| `POST` | `/raft/join` | New node join request (forwarded to leader if needed) |
| `POST` | `/raft/peer` | Membership broadcast from leader to followers |
| `GET` | `/status` | Node status: role, leader ID, current term |
| `GET` | `/health` | Liveness probe |

### REST gateway (port 8000)

| Method | Path | Body | Description |
|---|---|---|---|
| `POST` | `/redis/set` | `{"key","value","expire_ms"}` | Set with optional TTL |
| `POST` | `/redis/get` | `{"key"}` | Get value |
| `POST` | `/redis/del` | `{"key"}` | Delete key |
| `GET` | `/cluster/status` | — | Status of all Raft nodes |
| `GET` | `/health` | — | Liveness probe |

## Adding a node at runtime

Start a new container with `JOIN_ADDR` pointing to any live node:

```yaml
node4:
  build: .
  environment:
    - NODE=node4
    - PORT=5000
    - SELF_ADDR=http://node4:5000
    - JOIN_ADDR=http://node1:5000   # any alive node works
    - LOGFILE=/data/raft.log
    - STATEFILE=/data/raft.state
  volumes:
    - node4-data:/data
  networks:
    - raft-network
  command: ["./raft-redis", "redis"]
```

Join sequence:

1. `node4` starts, waits 500 ms for its HTTP server, then `POST /raft/join` → `node1`
2. `node1` forwards to the current leader if needed
3. Leader calls `AddPeer("node4", ...)`, sets `nextIndex[node4]=0`
4. Leader broadcasts `POST /raft/peer` to all existing followers
5. Leader immediately replicates the full log to `node4`
6. `node4` receives `AppendEntries`, syncs up, and joins as a follower

## Fault tolerance

| Scenario | Behaviour |
|---|---|
| Leader dies | Health checker detects 2 consecutive failures (~400 ms); surviving nodes clear stale `leaderId` and trigger re-election immediately |
| Follower dies | Leader stops replicating to it; quorum maintained as long as majority alive; dead node marked and health-checked every 2 s for recovery |
| Node recovers | Health checker detects recovery; if it was the leader it rejoins as a follower (higher term from new leader steps it down) |
| Network partition | Minority partition cannot commit (no quorum); majority elects new leader and continues |

Minimum nodes for fault tolerance: **3 nodes** tolerate 1 failure, **5 nodes** tolerate 2 failures.

## Project layout

```
go-redis-raft/
├── main.go            # Cobra CLI root
├── Dockerfile
├── docker-compose.yml
├── cmd/
│   ├── redis.go       # `redis` command — Raft node server
│   └── service.go     # `service` command — REST gateway
├── raft/
│   ├── node.go        # RaftNode lifecycle, AppendEntries, RequestVote
│   ├── election.go    # campaign(), quorum()
│   ├── client.go      # HTTP RPC client for peer communication
│   ├── types.go       # RPC arg/reply types, LogEntry alias
│   └── state.go       # Persistent currentTerm + votedFor
├── redis/
│   ├── redis.go       # RedisClient state machine (Set/Get/Delete + TTL)
│   └── service.go     # Command message types
└── wal/
    └── wal.go         # Binary write-ahead log (length-prefixed JSON)
```

## Building locally

```bash
go build -o raft-redis .

# Single-node mode (becomes leader immediately)
NODE=node1 PORT=5000 ./raft-redis redis

# In another terminal
NODE=svc PORT=8000 PEERS=node1=http://localhost:5000 ./raft-redis service
```

## Design notes

- **HTTP over gRPC** — peer RPCs use plain HTTP/JSON; no protobuf dependency
- **WAL format** — each entry is a 4-byte little-endian length followed by a JSON-encoded `LogEntry`
- **Quorum** — `floor((n+1)/2) + 1` where `n` is the number of peers (excluding self)
- **Write path** — `Propose()` appends to the leader WAL, immediately sends `AppendEntries` to all peers, then `WaitCommit()` polls until `commitIndex` reaches the entry's index before returning to the client
- **Health checker** — runs at 200 ms intervals; confirmed-dead nodes are skipped and probed for recovery every 2 s to avoid log spam and wasted connections
