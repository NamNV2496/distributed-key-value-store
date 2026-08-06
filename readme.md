# Distributed key-value store

A distributed key-value store built from scratch in Go, using the **Raft consensus algorithm** for replication and fault tolerance. No external Raft libraries — the full leader election, log replication, and cluster membership protocol is implemented by hand.

# How to start

```bash
docker compose -f docker-compose.multiraft.yml up -d
```

Access UI
```
http://localhost:6600/dashboard/
```


## Architecture

```mermaid
flowchart TB
    C["client"]

    subgraph routing["routing tier — stateless"]
        P1["proxy"]
        P2["proxy"]
    end

    subgraph storage["storage tier — stateful"]
        subgraph s0["shard-0"]
            N1["node1<br/>leader"]
            N2["node2"]
            N3["node3"]
        end
        subgraph s1["shard-1"]
            N4["node4"]
            N5["node5<br/>leader"]
            N6["node6"]
        end
    end

    D["dashboard"]

    C --> P1 & P2
    P1 --> N1
    P2 --> N5
    N1 <-->|Raft| N2
    N1 <-->|Raft| N3
    N5 <-->|Raft| N4
    N5 <-->|Raft| N6
    D -.->|reads only| N1 & N5
```

### Endpoint flow

```text
Clients
  │
  ▼
HTTP handlers
  │
  ▼
Command queue
  │
  ▼
Single execution loop
  │
  ├─ Raft proposal / commit
  └─ Local store update
        │
        ▼
   Raft peers / WAL
```


```mermaid
sequenceDiagram
    participant C as client
    participant P as shard_proxy
    participant F as node (follower)
    participant L as node (leader)
    participant R as các follower

    C->>P: POST /kv  SET user:1
    Note over P: hash → slot → shard sở hữu<br/>hop đầu = leader đã biết của shard đó
    P->>L: POST /kv  (tới một member)
    Note over L: chạy lại routing trên topology của chính nó
    L->>L: group cục bộ → Submit
    Note over L: leader → execution loop
    L->>L: Propose → ghi vào WAL
    L->>R: AppendEntries
    R-->>L: ack (quorum)
    Note over L: commit → apply → ghi lại kết quả
    L-->>P: kết quả
    P-->>C: {"status":"success","served_by":"node1"}

    Note over P,F: nếu thông tin leader ở proxy đã cũ
    P->>F: POST /kv
    F->>L: chuyển tiếp tới leader
    L-->>F: kết quả
    F-->>P: kết quả
```


### Follower-to-leader forwarding

```mermaid
flowchart TD
    F[Follower node] -->|POST /raft/command| L[Leader node]
    L --> Q[Single execution loop]
    Q --> R[Propose / replicate / commit]
    R --> A[Apply to local state]
    A --> RESP[Return result to client]
```

## Sharding (multi-Raft)

One Raft group means one leader serializing every write. Sharding splits the
keyspace across several independent groups, so each group elects, replicates
and commits on its own.

```text
                       POST /kv  {"cmd":"SET","args":{"key":"user:7",...}}
                                        │
                              slot = crc32(key) % 16384
                                        │
                        topology: slot -> shard -> member nodes
                    ┌───────────────────┴───────────────────┐
              shard-0 (Raft group)                    shard-1 (Raft group)
        node1(L)   node2(F)   node3(F)          node4(F)  node5(F)  node6(L)
```

- **Slots** — a key hashes to one of `SLOTS` slots (16384 by default). Slots are
  the unit of ownership, so rebalancing never rehashes individual keys. A key
  may carry a hash tag — `{user:42}:profile` and `{user:42}:sessions` hash only
  the tag, so related keys share a slot and therefore a group.
- **Shards** — each shard is a full Raft group with its own WAL, state file,
  elections and state machine. A node hosts one group per shard it replicates,
  all behind a single HTTP listener under `/shards/<shard-id>/…`.
- **Topology** — a version-stamped map of `slot -> shard` and `shard -> members`,
  persisted per node and pushed to every node when it changes.
- **Rebalancing** — a consistent hash ring with virtual nodes and a bounded load
  cap assigns slots to shards, so adding or removing a shard moves close to its
  fair share of slots and leaves the rest untouched.

### The routed API

`POST /kv` is the one endpoint a client needs. It hashes the command's key,
finds the shard that owns the slot, and runs the command there — locally if this
node hosts the shard, over HTTP if it does not.

```bash
curl -sX POST localhost:5001/kv \
  -d '{"cmd":"SET","args":{"key":"user:7","value":"alice"}}'
# {"node_id":"node1","shard":"shard-0","slot":9279,"command":"SET",
#  "status":"success","result":"OK","topology_version":1,"served_by":"node1"}

# any node answers for any key
curl -sX POST localhost:5004/kv -d '{"cmd":"GET","args":{"key":"user:7"}}'

# ?redirect=1 returns the owning shard instead of proxying, for a client
# that wants to cache the slot map itself (Redis Cluster's MOVED)
curl -sX POST 'localhost:5001/kv?redirect=1' -d '{"cmd":"GET","args":{"key":"user:7"}}'
```

### Rebalancing the hash ring

```bash
# where would a key go, and what does the map look like now
curl -s 'localhost:5001/cluster/locate?key=user:7'
curl -s localhost:5001/cluster/shards

# preview: what would adding a shard move?
curl -sX POST 'localhost:5001/cluster/rebalance?dry_run=1' \
  -d '{"shards":[ ...full desired shard set... ]}'

# add a shard and migrate its slots to it
curl -sX POST localhost:5001/cluster/shards -d '{
  "action":"add","shard":"shard-2",
  "members":{"node1":"http://node1:5000","node4":"http://node4:5000","node6":"http://node6:5000"}
}'

# drain and remove one
curl -sX POST localhost:5001/cluster/shards -d '{"action":"remove","shard":"shard-2"}'
```

### Routing tier as its own service (`proxy` command)

Every node already routes: `POST /kv` on any node hashes the key and forwards to
whichever shard owns it. The `proxy` command runs only that part, as a separate
process that hosts no shard, runs no Raft group and stores nothing.

```bash
docker compose -f docker-compose.multiraft.yml up --build
curl -sX POST localhost:6200/kv -d '{"cmd":"SET","args":{"key":"user:1","value":"a"}}'
curl -sX POST localhost:6200/kv -d '{"cmd":"GET","args":{"key":"user:1"}}'
curl -s  localhost:6200/cluster/locate?key=user:1   # slot, owning shard, first hop

# standalone, outside Docker
SEEDS=http://localhost:5001,http://localhost:5004 PORT=6200 go run . proxy
```


### A write

```mermaid
sequenceDiagram
    autonumber
    participant C as client
    participant PX as proxy/
    participant RT as routing/
    participant CL as cluster/
    participant SH as shard/
    participant RD as shard/redis/
    participant RF as shard/raft/
    participant WL as shard/raft/wal/
    participant DS as shard/redis/<br/>data_structure/
    participant F2 as node2<br/>shard/raft/

    C->>PX: POST /kv   SET {u1}:name

    rect rgba(128,128,128,0.07)
    Note over PX,CL: decision #1 — inside the proxy process
    PX->>PX: Proxy.HandleKV · json.Unmarshal → Command
    PX->>CL: Store.Get() → *cluster.Topology (v7)
    PX->>RT: Route(topo, cmd, FirstShard)
    RT->>RT: routingKey(cmd) → "{u1}:name"
    RT->>CL: HashSlot(key, 16384)
    CL->>CL: HashTag → "u1" · crc32 % 16384 → 5231
    CL-->>RT: 5231
    RT->>CL: topo.Owner(5231) → "shard-b"
    RT->>CL: topo.MigrationFor(5231)
    RT->>RD: IsWriteCommand("SET") → true
    Note over RT,RD: the reversed edge resp/ exists to remove
    RT-->>PX: Decision{Slot:5231, ShardID:"shard-b"}
    end

    PX->>PX: FirstHop("shard-b") → preferred member
    PX->>RT: PeerClient.PostToShardMember(shard-b, body, KVURL)
    RT->>RT: orderCandidates(members, prefer, skip)
    RT->>SH: POST /kv · X-Topology-Version: 7

    rect rgba(128,128,128,0.07)
    Note over SH,CL: decision #2 — inside the node process; this is the one that counts
    SH->>SH: Manager.HandleKV
    SH->>CL: Store.Get() → the node's own topology (v8)
    SH->>RT: Route(topo, cmd, m.anyLocalShard)
    RT-->>SH: Decision
    end

    SH->>SH: shardCommand(shardID, cmd)
    alt this node hosts shard-b
        SH->>RD: m.group("shard-b").SubmitCommand(ctx, cmd)
    else it does not
        SH->>RT: PeerClient.PostToShardMember(ShardCommandURL)
    end

    RD->>RF: GetLeaderID()
    alt this node is not the leader
        RD->>RD: forwardToLeader → POST /shards/shard-b/raft/command
    end
    RD->>RD: commandQueue <- req · runExecutionLoop · executeCommand
    RD->>RD: isWriteCommand("SET") → true

    RD->>RF: Propose("SET", body)
    RF->>WL: log.Append(cmd, term, index, data)
    WL->>WL: writeEntry + sync() — fsync
    WL-->>RF: ok
    RF->>F2: sendAppendEntries → AppendEntries RPC
    F2->>F2: term / PrevLogIndex / PrevLogTerm checks
    F2->>F2: wal.AppendBatch(entries) — one fsync for the batch
    F2-->>RF: Success · matchIndex
    RF->>RF: advanceCommitIndex — quorum ∧ current term
    RF-->>RD: (index, term)

    par apply goroutine
        RF->>RD: applyChan <- LogEntry
        RD->>RD: RunApplyLoop → applyEntry → EvalAndResponse
        RD->>DS: cmdSET(args) → Dict.Set
        DS-->>RD: "OK"
        RD->>RD: recordResult(index, "OK")
        RD->>RF: MarkApplied(index)
    and command goroutine
        RD->>RF: WaitApplied(ctx, index) — blocks here
    end

    RD->>RF: EntryTerm(index) == the term it proposed in?
    RD->>RD: TakeResult(index) → "OK"
    RD-->>SH: result
    SH->>RT: WriteJSON(KVResponse) · X-Topology-Version: 8
    RT->>RT: noticeTopologyVersion — 8 > 7 → fetch in background
    RT-->>PX: result, servedBy
    PX->>RT: WriteJSON(KVResponse{status:"success"})
    RT-->>C: 200
```

Three things the package view shows that the role view does not:

- **`routing/` plays two parts.** Steps 4–12 it is the *decision rule*, calling
  down into `cluster/`. Steps 14–16 it is the *transport*, going over the network.
  One folder, two jobs that have nothing to do with each other.
- **The `par` block is the piece that reads wrong in the source.** `RunApplyLoop`
  runs in its own goroutine; `executeCommand` only waits. `MarkApplied` on the
  left branch is what releases `WaitApplied` on the right.
- **`cluster/` stops appearing after step 19.** Once the shard is known the map has
  no further part to play; everything after is consensus.

### A read

```mermaid
sequenceDiagram
    autonumber
    participant C as client
    participant PX as proxy/
    participant RT as routing/
    participant CL as cluster/
    participant SH as shard/
    participant RD as shard/redis/
    participant RF as shard/raft/
    participant WL as shard/raft/wal/
    participant DS as shard/redis/<br/>data_structure/
    participant F2 as node2<br/>shard/raft/

    C->>PX: POST /kv   GET {u1}:name

    PX->>CL: Store.Get() → *cluster.Topology
    PX->>RT: Route(topo, cmd, FirstShard)
    RT->>CL: HashSlot → 5231 · Owner → "shard-b"
    RT->>CL: topo.MigrationFor(5231) → migrating
    RT->>RD: IsWriteCommand("GET") → false
    Note over RT: false ⇒ not refused.<br/>a migrating slot still serves reads from its current owner.
    RT-->>PX: Decision{Slot:5231, ShardID:"shard-b"}

    PX->>RT: PeerClient.PostToShardMember(shard-b, body, KVURL)
    RT->>SH: POST /kv · X-Topology-Version: 7
    SH->>CL: Store.Get()
    SH->>RT: Route again — the node's own topology
    RT-->>SH: Decision
    SH->>RD: SubmitCommand(ctx, cmd)

    RD->>RF: GetLeaderID()
    alt this node is not the leader
        RD->>RD: forwardToLeader
        Note over RD: a read travels to the leader too — a follower serves nothing
    end
    RD->>RD: executeCommand · isWriteCommand("GET") → false

    rect rgba(128,128,128,0.07)
    Note over RD,F2: the read barrier, in place of writing a log entry
    RD->>RF: ReadIndex(ctx)
    RF->>RF: role == Leader? idx := commitIndex (286)
    RF->>RF: confirmLeadership(ctx, term)
    alt hasQuorumLease() — lastAck still within electionTimeout
        RF->>RF: use the lease, send nothing
    else the lease has gone cold
        RF->>F2: empty AppendEntries (heartbeat) to every peer
        F2-->>RF: Success
        RF->>RF: votes >= quorum → still the leader
    end
    RF-->>RD: readIndex = 286
    end

    RD->>RF: WaitApplied(ctx, 286) — wait for the state machine to catch up
    RD->>RD: EvalAndResponse(cmd) · isReadOnlyCommand → RLock
    RD->>DS: cmdGET(args) → Dict.Get
    DS-->>RD: "nam"

    Note over RF,WL: nothing was proposed · the WAL is not touched
    RD-->>SH: "nam"
    SH->>RT: WriteJSON(KVResponse)
    RT-->>PX: result, servedBy
    PX->>RT: WriteJSON(KVResponse{status:"success"})
    RT-->>C: 200
```
