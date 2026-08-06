#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

WORKDIR="$(mktemp -d)"
PIDS=()
FAILURES=0

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

port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

pick_base() {
  local base=$1 limit=$2 span=$3 ok i
  while [[ "$base" -lt "$limit" ]]; do
    ok=1
    for ((i = 0; i < span; i++)); do
      port_free $((base + i)) || { ok=0; break; }
    done
    [[ "$ok" -eq 1 ]] && { echo "$base"; return 0; }
    base=$((base + span))
  done
  echo "could not find $span free ports" >&2
  return 1
}

NODES=(node1 node2 node3)
BASE="$(pick_base 5401 5600 3)"
echo "using ports $BASE-$((BASE + 2))"

port_of() { echo $((BASE + $1)); }
url() { echo "http://127.0.0.1:$(port_of "$1")"; }
addr_of() { echo "http://127.0.0.1:$(port_of "$1")"; }

KEYS=(alpha bravo charlie delta echo foxtrot golf hotel india juliet)
PREV_SHARD=()

check() {
  if [[ "$2" == "$3" ]]; then
    printf "  ok    %-50s %s\n" "$1" "$2"
  else
    printf "  FAIL  %-50s got %q want %q\n" "$1" "$2" "$3"
    FAILURES=$((FAILURES + 1))
  fi
}

kv() {
  curl -sS --max-time 10 -X POST "$(url 0)/kv" \
    -H 'Content-Type: application/json' -d "$1"
}

read_all() {
  local i key resp val shard note bad=0 moved=0
  for i in "${!KEYS[@]}"; do
    key="${KEYS[$i]}"
    resp="$(kv "{\"cmd\":\"GET\",\"args\":{\"key\":\"$key\"}}")"
    val="$(printf '%s' "$resp" | python3 -c "import json,sys;print(json.load(sys.stdin).get('result',''))")"
    shard="$(printf '%s' "$resp" | python3 -c "import json,sys;print(json.load(sys.stdin).get('shard',''))")"

    note=""
    if [[ -n "${PREV_SHARD[$i]:-}" && "${PREV_SHARD[$i]}" != "$shard" ]]; then
      note="   moved from ${PREV_SHARD[$i]}"
      moved=$((moved + 1))
    fi

    if [[ "$val" == "v$i" ]]; then
      printf "    %-9s = %-4s on %-8s%s\n" "$key" "$val" "$shard" "$note"
    else
      printf "    %-9s = %-9s on %-8s   <-- LOST, wanted v%d\n" \
        "$key" "${val:-<empty>}" "$shard" "$i"
      bad=$((bad + 1))
    fi
    PREV_SHARD[$i]="$shard"
  done
  LAST_MOVED="$moved"
  check "$1" "$bad" "0"
}

layout() {
  curl -s --max-time 5 "$(url 0)/cluster/shards" |
    python3 -c "
import json,sys
d=json.load(sys.stdin)
per=d['slots_per_shard']
print('    v%-3d total=%d  %s' % (
    d['version'], sum(per.values()),
    '  '.join(f'{k}={per[k]}' for k in sorted(per))))"
}

shard_ids() {
  curl -s --max-time 5 "$(url 0)/cluster/shards" |
    python3 -c "
import json,sys
print(','.join(sorted(json.load(sys.stdin)['slots_per_shard'])))"
}

total_slots() {
  curl -s --max-time 5 "$(url 0)/cluster/shards" |
    python3 -c "
import json,sys
print(sum(json.load(sys.stdin)['slots_per_shard'].values()))"
}

hosting() {
  curl -s --max-time 5 "$(url "$1")/cluster/shards" |
    python3 -c "
import json,sys
print(','.join(sorted(json.load(sys.stdin).get('hosting') or [])) or '-')"
}

rebalance() {
  curl -sS --max-time 60 -X POST "$(url 0)/cluster/shards" \
    -H 'Content-Type: application/json' -d "$1" |
    python3 -c "
import json,sys
d=json.load(sys.stdin)
print('    moved %d slots, %d keys, %d failures (v%d -> v%d)' % (
    d['migrated_slots'], d['keys_moved'], d['failures'],
    d['from_version'], d['to_version']))"
}

echo "== building =="
go build -o "$WORKDIR/raft-redis" .

SHARDS="shard-0:node1=$(addr_of 0);shard-1:node2=$(addr_of 1);shard-2:node3=$(addr_of 2)"

echo
echo "== starting with 3 shards, one node each =="
for i in 0 1 2; do
  node="${NODES[$i]}"
  mkdir -p "$WORKDIR/$node"
  NODE="$node" PORT="$(port_of "$i")" ADVERTISE="$(addr_of "$i")" \
    SHARDS="$SHARDS" SLOTS=16384 DATA_DIR="$WORKDIR/$node" THREADS=2 \
    "$WORKDIR/raft-redis" redis >"$WORKDIR/$node.log" 2>&1 &
  PIDS+=($!)
done

for i in 0 1 2; do
  node="${NODES[$i]}"
  ready=0
  for _ in $(seq 1 60); do
    if curl -s --max-time 1 "$(url "$i")/health" | grep -q "\"$node\""; then ready=1; break; fi
    sleep 0.2
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "$node did not come up. Log:" >&2
    cat "$WORKDIR/$node.log" >&2
    exit 1
  fi
done

for _ in $(seq 1 60); do
  curl -s --max-time 2 "$(url 0)/status" | grep -q '"leader_id":""' || break
  sleep 0.5
done

layout
check "shards present" "$(shard_ids)" "shard-0,shard-1,shard-2"

echo
echo "== STEP 1: write 10 keys, hash-routed across the three shards =="
for i in "${!KEYS[@]}"; do
  kv "{\"cmd\":\"SET\",\"args\":{\"key\":\"${KEYS[$i]}\",\"value\":\"v$i\"}}" >/dev/null
done
read_all "all 10 keys written and readable"

echo
echo "== STEP 2: remove shard-2, then read back every key written in step 1 =="
rebalance '{"action":"remove","shard":"shard-2"}'
layout
check "shard-2 is gone" "$(shard_ids)" "shard-0,shard-1"
check "every slot still owned" "$(total_slots)" "16384"
echo
read_all "all 10 keys survived the removal"
echo "    ($LAST_MOVED of 10 keys changed shard)"

echo
echo "== STEP 3: add shard-3, then read back every key again =="
rebalance "{\"action\":\"add\",\"shard\":\"shard-3\",\"members\":{\"node3\":\"$(addr_of 2)\"}}"
layout
check "shard-3 joined" "$(shard_ids)" "shard-0,shard-1,shard-3"
check "every slot still owned" "$(total_slots)" "16384"
echo
read_all "all 10 keys survived the addition"
echo "    ($LAST_MOVED of 10 keys changed shard)"

echo
echo "== STEP 4: add shard-4 on node1, which already hosts shard-0 =="
echo "   one node, two Raft groups — that is what makes this multi-raft"
rebalance "{\"action\":\"add\",\"shard\":\"shard-4\",\"members\":{\"node1\":\"$(addr_of 0)\"}}"
layout
check "shard-4 joined" "$(shard_ids)" "shard-0,shard-1,shard-3,shard-4"
check "every slot still owned" "$(total_slots)" "16384"
echo
read_all "all 10 keys survived the second addition"
echo "    ($LAST_MOVED of 10 keys changed shard)"

echo
echo "== which node hosts which shards now =="
for i in 0 1 2; do
  printf "    %-6s %s\n" "${NODES[$i]}" "$(hosting "$i")"
done

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "PASS — 3 shards -> removed shard-2 -> added shard-3 -> added shard-4."
  echo "       All 10 keys from step 1 stayed readable throughout,"
  echo "       and 16384 slots stayed owned at every version."
else
  echo "FAIL — $FAILURES check(s) failed"
  exit 1
fi
