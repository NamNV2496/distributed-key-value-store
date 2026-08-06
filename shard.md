# Shards: configuring, adding, removing and moving

A shard is one Raft group. It owns a subset of the 16384 hash slots, and every
key belongs to exactly one slot. A node can host several shards at once — that
is what "multi-raft" means here — so shards and nodes are configured separately.

There are two ways to change the shard set:

- **At startup**, through environment variables, when a cluster is first formed.
- **At runtime**, through the cluster API, on a cluster that is already serving.

The runtime path is the normal one. Startup configuration only decides what the
cluster looks like the first time it comes up; after that the topology is
persisted and a restart rejoins with whatever slot assignment it last accepted.

---

## 1. Startup configuration

These are read once, by the `redis` command, when a node boots.

| Variable | Default | What it does |
| --- | --- | --- |
| `SHARDS` | — | The whole shard layout, inline. See the format below. |
| `SHARDS_FILE` | — | The same layout, read from a JSON file. **Wins over `SHARDS`.** |
| `SHARD_ID` | `shard-0` | Name of the single shard formed from `PEERS` when neither of the above is set. |
| `SLOTS` | `16384` | How many hash slots the keyspace divides into. Must be identical on every node. |
| `SHARD_VNODES` | `160` | Virtual nodes per shard on the hash ring. Higher spreads slots more evenly. |
| `NODE` | `node1` | This node's ID. Must match the ID used in `SHARDS`. |
| `ADVERTISE` | `http://$NODE:5000` | The URL other nodes reach this one at. |
| `PORT` | `5000` | The cluster port this node listens on. |
| `DATA_DIR` | — | Where the Raft log, state and cached topology live. Set it, or a restart starts empty. |

### The `SHARDS` format

```
<shard-id>:<node-id>=<url>,<node-id>=<url>;<shard-id>:<node-id>=<url>,...
```

Members of one shard are separated by commas, shards by semicolons.

```bash
SHARDS="shard-0:node1=http://node1:5000,node2=http://node2:5000,node3=http://node3:5000;
        shard-1:node4=http://node4:5000,node5=http://node5:5000,node6=http://node6:5000"
```

Every node in the cluster is given the **same** `SHARDS` string. A node starts a
Raft group for each shard that names it, and simply serves the cluster API for
the ones that do not.

`docker-compose.multiraft.yml` does exactly this with a YAML anchor, so the
layout is written once and shared by all six nodes.

### Replicas versus shards

These are different axes, and it is worth being deliberate about both:

- More **members in a shard** = more copies of the same data. Survives failures.
  Three members tolerate one loss; five tolerate two.
- More **shards** = the keyspace split further. Adds capacity and write
  throughput, but no extra safety.

A shard with one member has no redundancy: lose that node and you lose its
slots. Use one-member shards for demos only.

---

## 2. Adding a shard

`POST /cluster/shards` on **any node's cluster port** (not the proxy, and not
the dashboard — neither runs a control plane).

```bash
curl -X POST http://node1:5000/cluster/shards \
  -H 'Content-Type: application/json' \
  -d '{
        "action": "add",
        "shard":  "shard-2",
        "members": {
          "node1": "http://node1:5000",
          "node4": "http://node4:5000",
          "node6": "http://node6:5000"
        }
      }'
```

This is a single call that does three things: recomputes the slot assignment,
starts the new Raft group on its members, and migrates the affected keys. It
returns when the data has moved.

| Field | Meaning |
| --- | --- |
| `action` | `"add"` or `"remove"`. |
| `shard` | The new shard's ID. Must not already exist. |
| `members` | node ID → base URL. At least one. May be nodes already hosting other shards. |
| `dry_run` | Optional. Plan and report, change nothing. |
| `max_slots` | Optional. Migrate at most this many slots, then stop and report the rest as `remaining_moves`. |

The response is a rebalance report:

```json
{
  "from_version": 1, "to_version": 31,
  "planned_moves": 6044, "migrated_slots": 6044, "remaining_moves": 0,
  "keys_moved": 1, "skipped_keys": 0, "failures": 0,
  "slots_before": {"shard-0": 8602, "shard-1": 7782},
  "slots_after":  {"shard-0": 5735, "shard-1": 4914, "shard-2": 5735},
  "unreachable_nodes": null,
  "duration": "2.9s"
}
```

Check `failures` and `unreachable_nodes` before considering it done. A node that
could not be reached did not get the new topology; it will pick it up when it
next talks to a peer, but until then it routes by the old map.

---

## 3. Preview before committing

Either endpoint accepts a dry run. Nothing is changed and no keys move.

```bash
curl -X POST 'http://node1:5000/cluster/rebalance?dry_run=1' \
  -H 'Content-Type: application/json' \
  -d '{"shards": [
        {"id":"shard-0","members":{"node1":"http://node1:5000"}},
        {"id":"shard-1","members":{"node2":"http://node2:5000"}},
        {"id":"shard-2","members":{"node3":"http://node3:5000"}}
      ]}'
```

Read `planned_moves`, `slots_before` and `slots_after` to see the cost before
paying it.

---

## 4. Removing a shard

```bash
curl -X POST http://node1:5000/cluster/shards \
  -H 'Content-Type: application/json' \
  -d '{"action": "remove", "shard": "shard-2"}'
```

Its slots are redistributed to the shards that remain, and its keys migrate with
them before the group is stopped. No `members` field is needed.

The last remaining shard cannot be removed — there would be nowhere for its
slots to go.

---

## 5. Moving a shard onto different nodes

Changing an existing shard's members is a different operation from adding or
removing one, and it goes through `/cluster/rebalance` with the full desired
layout:

```bash
curl -X POST http://node1:5000/cluster/rebalance \
  -H 'Content-Type: application/json' \
  -d '{"shards": [
        {"id":"shard-0","members":{
            "node1":"http://node1:5000",
            "node2":"http://node2:5000",
            "node4":"http://node4:5000"}},
        {"id":"shard-1","members":{
            "node4":"http://node4:5000",
            "node5":"http://node5:5000",
            "node6":"http://node6:5000"}}
      ]}'
```

Send the shards you want to end up with — the ones you leave unchanged must
still be listed, or they will be read as removals.

`planned_moves` will be `0`, because no slot changed shard. The work is a Raft
membership change, which converges in the background rather than before the
response returns. Watch it on `/status`:

```bash
curl -s http://node1:5000/status | jq '.shards[] | {shard_id, role, members, learners}'
```

A node joining a shard appears first under `learners`, then moves to `members`
once it has caught up. Departing nodes disappear one at a time. This is
deliberate and is what makes the move safe: a joining node starts with an empty
log, so it is admitted without a vote until it holds everything the group has
committed. Admitting it as a voter immediately would let empty nodes outvote the
ones holding the data.

Because changes are applied one member at a time, a move of three members takes
several rounds. It is normal for `/status` to show an intermediate set.

---

## 6. Inspecting the current layout

```bash
curl -s http://node1:5000/cluster/shards | jq
```

```json
{
  "version": 31,
  "slots_per_shard": {"shard-0": 5735, "shard-1": 4914, "shard-2": 5735},
  "hosting": ["shard-0", "shard-2"],
  "shards": { "...": "full member map" }
}
```

`hosting` is the shards this particular node runs; the rest is cluster-wide.
The slot counts should always sum to `SLOTS`.

Related endpoints:

| Endpoint | Use |
| --- | --- |
| `GET /cluster/topology` | The full map. Add `?summary=1` for a compact form. |
| `GET /cluster/locate?key=foo` | Which slot and shard a given key belongs to. |
| `GET /cluster/status` | Every shard, its leader, and every node's reachability. |
| `GET /status` | This node only, including each group's Raft membership. |

---

## 7. Hash tags

Keys that share a `{...}` tag hash to the same slot, so they always live on the
same shard and move together:

```
{user:42}:profile   -> slot 3462
{user:42}:sessions  -> slot 3462
{user:42}:cart      -> slot 3462
```

Use this for keys that are read or written together. Without a tag, related keys
scatter across shards.

---

## 8. Runnable examples

Three scripts exercise the paths above end to end and assert the results:

| Script | What it shows |
| --- | --- |
| `scripts/shard-lifecycle-demo.sh` | 3 shards → remove one → add one → add another, reading all 10 keys back at each step |
| `scripts/shard-membership-demo.sh` | Moving a shard onto a different node set in place, without losing data |
| `scripts/multi-raft-demo.sh` | Two shards with three replicas each, a proxy, a dashboard, and a live rebalance |

Each picks free ports, builds the binary, runs a cluster in a temp directory and
cleans up after itself.

---

## 9. Things that will bite

- **`SLOTS` must match on every node.** Different values mean different slot
  maths and keys landing in the wrong place. It cannot be changed on a live
  cluster without moving everything.
- **Set `DATA_DIR`.** Without it the Raft log and the cached topology go to the
  working directory, and a container restart starts from nothing.
- **The proxy and dashboard have no control plane.** `POST /cluster/rebalance`
  against them is a 404 by design. Always target a node's cluster port.
- **One-member shards have no redundancy.** Fine for the demos, not for anything
  you care about.
- **Check `failures` and `unreachable_nodes`** in the report rather than
  assuming a 200 means every node has the new map.
