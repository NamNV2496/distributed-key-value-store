#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

NODES=(node1 node2 node3 node4 node5 node6)

# Six nodes, then one proxy and one dashboard — each of the three roles gets its
# own process, which is the point the second half of this script demonstrates.
PORT_COUNT=$((${#NODES[@]} + 2))

port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

find_base_port() {
  local base=$1
  local limit=$((base + 500))
  local i ok
  while [[ "$base" -lt "$limit" ]]; do
    ok=1
    for ((i = 0; i < PORT_COUNT; i++)); do
      port_free $((base + i)) || { ok=0; break; }
    done
    [[ "$ok" -eq 1 ]] && { echo "$base"; return 0; }
    base=$((base + 10))
  done
  echo "no free range of $PORT_COUNT ports found near $1" >&2
  return 1
}

BASE_PORT="${BASE_PORT:-$(find_base_port 5201)}"
PROXY_PORT=$((BASE_PORT + ${#NODES[@]}))
DASH_PORT=$((BASE_PORT + ${#NODES[@]} + 1))
echo "using ports $BASE_PORT-$((BASE_PORT + PORT_COUNT - 1))"
echo "  nodes $BASE_PORT-$((BASE_PORT + ${#NODES[@]} - 1)) · proxy $PROXY_PORT · dashboard $DASH_PORT"

failures=0
check() { # check <description> <actual> <expected>
  if [[ "$2" == "$3" ]]; then
    printf '  ok    %-52s %s\n' "$1" "$2"
  else
    printf '  FAIL  %-52s %s (want %s)\n' "$1" "$2" "$3"
    failures=$((failures + 1))
  fi
}

code() { curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$@"; }

# The value checks read raw JSON rather than piping through jq, so this script
# behaves the same on a machine without it.
json_num() { sed -n "s/.*\"$1\":\([0-9][0-9]*\).*/\1/p" | head -1; }

check_result() { # check_result <description> <json> <expected value>
  case "$2" in
    *"\"result\":\"$3\""*) printf '  ok    %-52s %s\n' "$1" "$3" ;;
    *) printf '  FAIL  %-52s %s\n' "$1" "$2"; failures=$((failures + 1)) ;;
  esac
}

check_has() { # check_has <description> <haystack> <needle>
  case "$2" in
    *"$3"*) printf '  ok    %-52s %s\n' "$1" "found $3" ;;
    *) printf '  FAIL  %-52s %s missing from %s\n' "$1" "$3" "$2"; failures=$((failures + 1)) ;;
  esac
}

port_of() { echo $((BASE_PORT + $1)); }
addr_of() { echo "http://127.0.0.1:$(port_of "$1")"; }

SHARDS="shard-0:node1=$(addr_of 0),node2=$(addr_of 1),node3=$(addr_of 2);"
SHARDS+="shard-1:node4=$(addr_of 3),node5=$(addr_of 4),node6=$(addr_of 5)"
NEW_SHARD_MEMBERS="{\"node1\":\"$(addr_of 0)\",\"node4\":\"$(addr_of 3)\",\"node6\":\"$(addr_of 5)\"}"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multi-raft-demo-XXXXXX")"
PIDS=()

cleanup() {
  echo
  echo "== shutting down =="
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

url() { echo "http://127.0.0.1:$(port_of $(($1 - 1)))"; }

jsonify() {
  if command -v jq >/dev/null 2>&1; then jq "$@"; else cat; fi
}

kv() {
  curl -sS -X POST "$(url "$1")/kv" -H 'Content-Type: application/json' -d "$2"
}

# pkv sends a command through the proxy. Same request body and same response
# shape as a node's /kv, which is what lets a client point at either.
pkv() {
  curl -sS -X POST "http://127.0.0.1:$PROXY_PORT/kv" -H 'Content-Type: application/json' -d "$1"
}

wait_healthy() { # wait_healthy <url> <what>
  for _ in $(seq 1 50); do
    if curl -sS --max-time 1 "$1/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "$2 never became healthy at $1" >&2
  return 1
}

echo "== building =="
go build -o "$WORKDIR/raft-redis" .

echo "== starting 6 nodes: 2 shards x 3 replicas =="
for i in "${!NODES[@]}"; do
  node="${NODES[$i]}"
  port="$(port_of "$i")"
  mkdir -p "$WORKDIR/$node"
  NODE="$node" PORT="$port" ADVERTISE="http://127.0.0.1:$port" \
    SHARDS="$SHARDS" SLOTS=16384 DATA_DIR="$WORKDIR/$node" THREADS=2 \
    "$WORKDIR/raft-redis" redis >"$WORKDIR/$node.log" 2>&1 &
  PIDS+=($!)
done

for i in "${!NODES[@]}"; do
  node="${NODES[$i]}"
  port="$(port_of "$i")"
  ready=0
  for _ in $(seq 1 50); do
    # -s, not -sS: a connection refusal here is the expected state while the
    # node is still binding its port, not something to print at the operator.
    if curl -s --max-time 1 "http://127.0.0.1:$port/health" | grep -q "\"$node\""; then
      ready=1
      break
    fi
    sleep 0.2
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "$node did not come up on port $port. Log:" >&2
    cat "$WORKDIR/$node.log" >&2
    exit 1
  fi
done
echo "waiting for every shard to elect a leader"
for attempt in $(seq 1 60); do
  pending=0
  for i in "${!NODES[@]}"; do
    if curl -s --max-time 2 "$(url $((i + 1)))/status" | grep -q '"leader_id":""'; then
      pending=$((pending + 1))
    fi
  done
  [[ "$pending" -eq 0 ]] && break
  sleep 0.5
done
if [[ "$pending" -ne 0 ]]; then
  echo "$pending node(s) still without a leader after 30s" >&2
  exit 1
fi

echo
echo "== starting the other two roles: a proxy and a dashboard =="
SEEDS="$(addr_of 0),$(addr_of 3)"

mkdir -p "$WORKDIR/proxy"
PROXY_ID=proxy1 PORT="$PROXY_PORT" SEEDS="$SEEDS" TOPOLOGY_REFRESH=1s DATA_DIR="$WORKDIR/proxy" \
  "$WORKDIR/raft-redis" proxy >"$WORKDIR/proxy.log" 2>&1 &
PIDS+=($!)

PORT="$DASH_PORT" SEEDS="$SEEDS" \
  "$WORKDIR/raft-redis" dashboard >"$WORKDIR/dashboard.log" 2>&1 &
PIDS+=($!)

if ! wait_healthy "http://127.0.0.1:$PROXY_PORT" proxy; then cat "$WORKDIR/proxy.log" >&2; exit 1; fi
if ! wait_healthy "http://127.0.0.1:$DASH_PORT" dashboard; then cat "$WORKDIR/dashboard.log" >&2; exit 1; fi
echo "  proxy hosts no shard, holds no data, and reads the topology from $SEEDS"
curl -sS "http://127.0.0.1:$PROXY_PORT/status" | jsonify -c '{node_id, role, topology_version, source, first_hop}'

echo
echo "== who is the leader of each shard =="
for i in "${!NODES[@]}"; do
  node="${NODES[$i]}"
  printf '%-6s ' "$node"
  curl -sS "$(url $((i + 1)))/status" |
    (command -v jq >/dev/null 2>&1 &&
      jq -c '[.shards[] | {shard: .shard_id, role: .role, leader: .leader_id, slots: .slots}]' ||
      cat)
done

echo
echo "== writing 8 keys through node1; each one is hash-routed to its shard =="
for i in $(seq 1 8); do
  kv 1 "{\"cmd\":\"SET\",\"args\":{\"key\":\"user:$i\",\"value\":\"v$i\"}}" |
    (command -v jq >/dev/null 2>&1 &&
      jq -c '{key: "user:'"$i"'", slot, shard, served_by, result}' || cat)
done

echo
echo "== the same keys read back from node4, which replicates the other shard =="
for i in $(seq 1 8); do
  kv 4 "{\"cmd\":\"GET\",\"args\":{\"key\":\"user:$i\"}}" |
    (command -v jq >/dev/null 2>&1 && jq -c '{slot, shard, served_by, result}' || cat)
done

echo
echo "== the same 8 keys, this time routed by the proxy instead of by a node =="
for i in $(seq 1 8); do
  pkv "{\"cmd\":\"GET\",\"args\":{\"key\":\"user:$i\"}}" |
    (command -v jq >/dev/null 2>&1 && jq -c '{slot, shard, node_id, served_by, result}' || cat)
done

echo
echo "== a write that only ever touches the proxy =="
pkv '{"cmd":"SET","args":{"key":"via-proxy","value":"ok"}}' |
  jsonify -c '{slot, shard, node_id, served_by, result}'
check_result "value readable back through the proxy" \
  "$(pkv '{"cmd":"GET","args":{"key":"via-proxy"}}')" "ok"
check_result "and through a node, which is where it actually lives" \
  "$(kv 1 '{"cmd":"GET","args":{"key":"via-proxy"}}')" "ok"

echo
echo "== the three roles are separate processes, and their surfaces prove it =="
P="http://127.0.0.1:$PROXY_PORT"
D="http://127.0.0.1:$DASH_PORT"
N="$(url 1)"

check "proxy: POST /kv"                  "$(code -X POST "$P/kv" -d '{"cmd":"PING"}')"        200
check "proxy: GET /cluster/topology"     "$(code "$P/cluster/topology")"                      200
check "proxy: POST /cluster/topology"    "$(code -X POST "$P/cluster/topology" -d '{}')"      405
check "proxy: POST /cluster/rebalance"   "$(code -X POST "$P/cluster/rebalance" -d '{}')"     404
check "proxy: POST /raft/append"         "$(code -X POST "$P/raft/append" -d '{}')"           404
check "proxy: GET /dashboard/"           "$(code "$P/dashboard/")"                            404
check "dashboard: GET /dashboard/"       "$(code "$D/dashboard/")"                            200
check "dashboard: GET /cluster/status"   "$(code "$D/cluster/status")"                        200
check "dashboard: POST /cluster/topology" "$(code -X POST "$D/cluster/topology" -d '{}')"     405
check "dashboard: POST /kv"              "$(code -X POST "$D/kv" -d '{"cmd":"PING"}')"        404
check "node: POST /cluster/rebalance?dry_run=1" "$(code -X POST "$N/cluster/rebalance?dry_run=1" -d '{}')" 200
check "node: GET /cluster/status"        "$(code "$N/cluster/status")"                        200
# Only the dashboard process serves a UI. A node exposes the read-only endpoints
# the dashboard polls, but no assets — so no process registers the read-only and
# writable halves of the cluster API on one mux.
check "node: GET /dashboard/"            "$(code "$N/dashboard/")"                            404

echo
echo "== where does a key live? (hash tags pin related keys together) =="
curl -sS "$(url 1)/cluster/locate?key=%7Buser:42%7D:profile" | jsonify -c '{key, hash_tag, slot, shard}'
curl -sS "$(url 1)/cluster/locate?key=%7Buser:42%7D:sessions" | jsonify -c '{key, hash_tag, slot, shard}'

echo
echo "== slots per shard right now =="
curl -sS "$(url 1)/cluster/shards" | jsonify -c '{version, slots_per_shard}'

echo
echo "== dry run: what would adding shard-2 move? =="
curl -sS -X POST "$(url 1)/cluster/rebalance?dry_run=1" \
  -H 'Content-Type: application/json' \
  -d "{\"shards\": [
    {\"id\":\"shard-0\",\"members\":{\"node1\":\"$(addr_of 0)\",\"node2\":\"$(addr_of 1)\",\"node3\":\"$(addr_of 2)\"}},
    {\"id\":\"shard-1\",\"members\":{\"node4\":\"$(addr_of 3)\",\"node5\":\"$(addr_of 4)\",\"node6\":\"$(addr_of 5)\"}},
    {\"id\":\"shard-2\",\"members\":$NEW_SHARD_MEMBERS}
  ]}" | jsonify -c '{planned_moves, slots_before, slots_after}'

echo
echo "== rebalancing for real: adding shard-2 =="
curl -sS -X POST "$(url 1)/cluster/shards" \
  -H 'Content-Type: application/json' \
  -d "{\"action\":\"add\",\"shard\":\"shard-2\",\"members\":$NEW_SHARD_MEMBERS}" |
  jsonify -c '{planned_moves, migrated_slots, keys_moved, failures, from_version, to_version, duration, unreachable_nodes}'

echo
echo "== slots per shard after the rebalance =="
curl -sS "$(url 1)/cluster/shards" | jsonify -c '{version, slots_per_shard}'

echo
echo "== the new shard elected its own leader =="
for i in 1 4 6; do
  printf 'node%s ' "$i"
  curl -sS "$(url "$i")/status" |
    (command -v jq >/dev/null 2>&1 &&
      jq -c '[.shards[] | select(.shard_id == "shard-2") | {shard: .shard_id, role: .role, leader: .leader_id, slots: .slots}]' ||
      cat)
done

echo
echo "== nobody pushes a topology to the proxy; it catches up on its own =="
node_version="$(curl -sS "$(url 1)/cluster/topology?summary=1" | json_num version)"
proxy_version=""
for _ in $(seq 1 40); do
  proxy_version="$(curl -sS "http://127.0.0.1:$PROXY_PORT/cluster/topology?summary=1" | json_num version)"
  [[ "$proxy_version" == "$node_version" ]] && break
  sleep 0.25
done
check "proxy topology version caught up to the cluster's" "$proxy_version" "$node_version"
check_has "proxy's own map now names the new shard" \
  "$(curl -sS "http://127.0.0.1:$PROXY_PORT/cluster/topology?summary=1")" "shard-2"
check_has "so does the dashboard's, read from the same nodes" \
  "$(curl -sS "http://127.0.0.1:$DASH_PORT/cluster/topology")" "shard-2"
curl -sS "http://127.0.0.1:$PROXY_PORT/status" | jsonify -c '{topology_version, first_hop}'

echo
echo "== every key is still readable, from every node and through the proxy =="
for i in $(seq 1 8); do
  for n in 1 2 3 4 5 6; do
    got="$(kv "$n" "{\"cmd\":\"GET\",\"args\":{\"key\":\"user:$i\"}}")"
    case "$got" in
      *"\"result\":\"v$i\""*) ;;
      *) echo "  MISMATCH user:$i via node$n -> $got"; failures=$((failures + 1)) ;;
    esac
  done
  got="$(pkv "{\"cmd\":\"GET\",\"args\":{\"key\":\"user:$i\"}}")"
  case "$got" in
    *"\"result\":\"v$i\""*) ;;
    *) echo "  MISMATCH user:$i via proxy -> $got"; failures=$((failures + 1)) ;;
  esac
done

echo
if [[ "$failures" -eq 0 ]]; then
  echo "PASS — 8 keys readable from all 6 nodes and through the proxy after the rebalance,"
  echo "       and each of the three roles exposed only its own surface."
else
  echo "FAIL — $failures check(s) did not match" >&2
  exit 1
fi
