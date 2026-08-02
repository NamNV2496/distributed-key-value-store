#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-5001}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT}}"
TIMEOUT="${TIMEOUT:-20}"
SERVER_LOG="${SERVER_LOG:-/tmp/go-redis-raft-http.log}"
SERVER_PID=""
DATA_DIR=""
STARTED_SERVER=0

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "$DATA_DIR" && -d "$DATA_DIR" ]]; then
    rm -rf "$DATA_DIR"
  fi
}
trap cleanup EXIT

ensure_server() {
  if curl -sS --max-time 2 "$BASE_URL/health" >/dev/null 2>&1; then
    echo "NOTE: reusing the server already listening on $BASE_URL."
    echo "      Its store may already hold keys, so results below reflect"
    echo "      pre-existing state rather than a clean run."
    echo
    return 0
  fi

  # Run against a throwaway WAL + state file. The server otherwise defaults to
  # .raft-<NODE>.log / .raft-<NODE>.state in the repo root, which are checked
  # into git: running this script would dirty them, and — now that a restart
  # replays the log into the store — every later run would start from whatever
  # the previous run left behind instead of from empty.
  DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/go-redis-raft-XXXXXX")"

  echo "Starting local server on $BASE_URL (data dir: $DATA_DIR)"
  NODE=node1 PORT="$PORT" THREADS=1 \
    LOGFILE="$DATA_DIR/raft.log" STATEFILE="$DATA_DIR/raft.state" \
    go run . redis >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  STARTED_SERVER=1

  for _ in $(seq 1 30); do
    if curl -sS --max-time 2 "$BASE_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "Failed to start server. Log:" >&2
      cat "$SERVER_LOG" >&2
      return 1
    fi
    sleep 1
  done

  echo "Server did not become healthy. Log:" >&2
  cat "$SERVER_LOG" >&2
  return 1
}

post_cmd() {
  local label="$1"
  local body="$2"
  echo "== $label =="
  curl -sS --max-time "$TIMEOUT" -X POST "$BASE_URL/raft/command" \
    -H 'Content-Type: application/json' \
    -d "$body" || true
  echo
  echo
}

# ---------------------------------------------------------------------------
# Each section sets up its own fixtures before reading them back, so a result
# is only empty when the command is genuinely broken. Destructive commands run
# last within their section.
# ---------------------------------------------------------------------------

cmds_string() {
  post_cmd "PING" '{"cmd":"PING"}'
  post_cmd "SET" '{"cmd":"SET","args":{"key":"demo","value":"hello"}}'
  post_cmd "GET" '{"cmd":"GET","args":{"key":"demo"}}'
  post_cmd "TTL (no expiry set, expect -1)" '{"cmd":"TTL","args":{"key":"demo"}}'
  post_cmd "EXPIRE" '{"cmd":"EXPIRE","args":{"key":"demo","ttl":"120"}}'
  post_cmd "TTL (after EXPIRE, expect ~120)" '{"cmd":"TTL","args":{"key":"demo"}}'
  post_cmd "INCR" '{"cmd":"INCR","args":{"key":"counter"}}'
  post_cmd "INCR again (expect 2)" '{"cmd":"INCR","args":{"key":"counter"}}'
  # DEL is destructive, so it runs after every read of "demo" above.
  post_cmd "DEL (expect 1)" '{"cmd":"DEL","args":{"key":"demo"}}'
  post_cmd "GET after DEL (expect empty)" '{"cmd":"GET","args":{"key":"demo"}}'
  # Variadic form: key plus numbered extras, as used by BF_MADD.
  post_cmd "SET a,b for multi-DEL" '{"cmd":"SET","args":{"key":"tmp_a","value":"1"}}'
  post_cmd "SET b" '{"cmd":"SET","args":{"key":"tmp_b","value":"1"}}'
  post_cmd "DEL tmp_a,tmp_b,missing (expect 2)" '{"cmd":"DEL","args":{"key":"tmp_a","1":"tmp_b","2":"missing"}}'
}

cmds_set() {
  post_cmd "SADD alice" '{"cmd":"SADD","args":{"key":"myset","value":"alice"}}'
  post_cmd "SADD bob" '{"cmd":"SADD","args":{"key":"myset","value":"bob"}}'
  post_cmd "SCARD (expect 2)" '{"cmd":"SCARD","args":{"key":"myset"}}'
  post_cmd "SISMEMBER alice" '{"cmd":"SISMEMBER","args":{"key":"myset","value":"alice"}}'
  post_cmd "SMISMEMBER alice" '{"cmd":"SMISMEMBER","args":{"key":"myset","value":"alice"}}'
  post_cmd "SMEMBERS" '{"cmd":"SMEMBERS","args":{"key":"myset"}}'
  post_cmd "SRAND" '{"cmd":"SRAND","args":{"key":"myset","value":"1"}}'
  post_cmd "SREM bob" '{"cmd":"SREM","args":{"key":"myset","value":"bob"}}'
  post_cmd "SCARD after SREM (expect 1)" '{"cmd":"SCARD","args":{"key":"myset"}}'
}

cmds_zset() {
  post_cmd "ZADD alice 100" '{"cmd":"ZADD","args":{"key":"board","score":"100","member":"alice"}}'
  post_cmd "ZADD bob 200" '{"cmd":"ZADD","args":{"key":"board","score":"200","member":"bob"}}'
  # Reads happen while both members still exist.
  post_cmd "ZCARD (expect 2)" '{"cmd":"ZCARD","args":{"key":"board"}}'
  post_cmd "ZSCORE alice (expect 100)" '{"cmd":"ZSCORE","args":{"key":"board","member":"alice"}}'
  post_cmd "ZRANK alice" '{"cmd":"ZRANK","args":{"key":"board","member":"alice"}}'
  post_cmd "ZREVRANK alice" '{"cmd":"ZREVRANK","args":{"key":"board","member":"alice"}}'
  post_cmd "ZRANGE 0..-1" '{"cmd":"ZRANGE","args":{"key":"board","start":"0","stop":"-1","withscores":"true"}}'
  post_cmd "ZCOUNT" '{"cmd":"ZCOUNT","args":{"key":"board","min":"0","max":"1000"}}'
  # ZINCRBY takes "increment", not "score".
  post_cmd "ZINCRBY alice +5 (expect 105)" '{"cmd":"ZINCRBY","args":{"key":"board","increment":"5","member":"alice"}}'
  post_cmd "ZREM alice" '{"cmd":"ZREM","args":{"key":"board","member":"alice"}}'
  post_cmd "ZCARD after ZREM (expect 1)" '{"cmd":"ZCARD","args":{"key":"board"}}'
}

cmds_geo() {
  # Both members must exist before GEODIST/GEOHASH/GEOPOS can say anything.
  post_cmd "GEOADD Palermo" '{"cmd":"GEOADD","args":{"key":"cities","longitude":"13.361389","latitude":"38.115556","member":"Palermo"}}'
  post_cmd "GEOADD Catania" '{"cmd":"GEOADD","args":{"key":"cities","longitude":"15.087269","latitude":"37.502669","member":"Catania"}}'
  post_cmd "GEODIST Palermo<->Catania (expect ~166 km)" '{"cmd":"GEODIST","args":{"key":"cities","member1":"Palermo","member2":"Catania","unit":"km"}}'
  post_cmd "GEOHASH" '{"cmd":"GEOHASH","args":{"key":"cities","m1":"Palermo","m2":"Catania"}}'
  post_cmd "GEOPOS" '{"cmd":"GEOPOS","args":{"key":"cities","m1":"Palermo"}}'
  # GEOSEARCH takes its radius in METRES: 200000 m = 200 km, which covers both.
  post_cmd "GEOSEARCH 200km from Palermo (expect both)" '{"cmd":"GEOSEARCH","args":{"key":"cities","frommember":"Palermo","radius":"200000"}}'
  post_cmd "GEOSEARCH 200m from Palermo (expect Palermo only)" '{"cmd":"GEOSEARCH","args":{"key":"cities","frommember":"Palermo","radius":"200"}}'
}

cmds_bloom() {
  post_cmd "BF_RESERVE" '{"cmd":"BF_RESERVE","args":{"key":"bf","errRate":"0.01","capacity":"10000"}}'
  post_cmd "BF_INFO" '{"cmd":"BF_INFO","args":{"key":"bf"}}'
  post_cmd "BF_MADD foo,bar" '{"cmd":"BF_MADD","args":{"key":"bf","1":"foo","2":"bar"}}'
  post_cmd "BF_EXISTS foo (expect 1)" '{"cmd":"BF_EXISTS","args":{"key":"bf","item":"foo"}}'
  post_cmd "BF_MEXISTS foo,missing (expect 1,0)" '{"cmd":"BF_MEXISTS","args":{"key":"bf","1":"foo","2":"missing"}}'
}

cmds_cms() {
  post_cmd "CMS_INITBYDIM" '{"cmd":"CMS_INITBYDIM","args":{"key":"cms","width":"2000","height":"7"}}'
  # CMS_INCRBY requires an odd argument count of at least 3.
  post_cmd "CMS_INCRBY +5" '{"cmd":"CMS_INCRBY","args":{"key":"cms","item":"foo","value":"5"}}'
  # CMS_QUERY requires at least 2 arguments.
  post_cmd "CMS_QUERY (expect 5)" '{"cmd":"CMS_QUERY","args":{"key":"cms","item":"foo"}}'
}

# post_json posts a command and prints only the raw response, so a caller can
# pull a field out of it.
post_json() {
  curl -sS --max-time "$TIMEOUT" -X POST "$BASE_URL/raft/command" \
    -H 'Content-Type: application/json' -d "$1"
}

# sub_id reads result.subscriber_id from a response on stdin.
sub_id() {
  python3 -c 'import sys, json; print(json.load(sys.stdin)["result"]["subscriber_id"])'
}

cmds_pubsub() {
  local a b p resp

  # SUBSCRIBE hands back a subscriber_id. That id is the handle a client uses
  # to collect its messages with POLL — without it, published messages have
  # nowhere to go.
  echo "== SUBSCRIBE news (subscriber A) =="
  resp=$(post_json '{"cmd":"SUBSCRIBE","args":{"channel":"news"}}'); echo "$resp"; echo
  a=$(printf '%s' "$resp" | sub_id)

  echo "== SUBSCRIBE news (subscriber B — a second one on the same channel) =="
  resp=$(post_json '{"cmd":"SUBSCRIBE","args":{"channel":"news"}}'); echo "$resp"; echo
  b=$(printf '%s' "$resp" | sub_id)

  echo "== PSUBSCRIBE news.* (subscriber P) =="
  resp=$(post_json '{"cmd":"PSUBSCRIBE","args":{"pattern":"news.*"}}'); echo "$resp"; echo
  p=$(printf '%s' "$resp" | sub_id)

  post_cmd "PUBLISH news (expect 2 — A and B, not the pattern)" \
    '{"cmd":"PUBLISH","args":{"channel":"news","message":"hello"}}'
  post_cmd "POLL A (expect the message)" "{\"cmd\":\"POLL\",\"args\":{\"id\":\"$a\"}}"
  post_cmd "POLL B (expect its own copy)" "{\"cmd\":\"POLL\",\"args\":{\"id\":\"$b\"}}"
  post_cmd "POLL A again (expect empty — already drained)" "{\"cmd\":\"POLL\",\"args\":{\"id\":\"$a\"}}"

  post_cmd "PUBLISH news.eu (expect 1 — pattern only)" \
    '{"cmd":"PUBLISH","args":{"channel":"news.eu","message":"world"}}'
  post_cmd "POLL P (expect a pmessage)" "{\"cmd\":\"POLL\",\"args\":{\"id\":\"$p\"}}"

  post_cmd "UNSUBSCRIBE A from news" "{\"cmd\":\"UNSUBSCRIBE\",\"args\":{\"id\":\"$a\",\"channel\":\"news\"}}"
  post_cmd "PUBLISH news (expect 1 — only B remains)" \
    '{"cmd":"PUBLISH","args":{"channel":"news","message":"again"}}'

  post_cmd "PUNSUBSCRIBE P from news.*" "{\"cmd\":\"PUNSUBSCRIBE\",\"args\":{\"id\":\"$p\",\"pattern\":\"news.*\"}}"
  post_cmd "PUBLISH news.eu (expect 0)" '{"cmd":"PUBLISH","args":{"channel":"news.eu","message":"nobody"}}'

  post_cmd "PUBSUB_CHANNELS" '{"cmd":"PUBSUB_CHANNELS"}'
}

cmds_ratelimit() {
  # Both commands require key, limit and window_ms; the limiter type is
  # optional and defaults to "fixed".
  post_cmd "RATELIMIT_INIT limit=3/60s" '{"cmd":"RATELIMIT_INIT","args":{"key":"req","limit":"3","window_ms":"60000","type":"fixed"}}'
  post_cmd "RATELIMIT_CHECK #1 (expect allowed)" '{"cmd":"RATELIMIT_CHECK","args":{"key":"req","limit":"3","window_ms":"60000","type":"fixed"}}'
  post_cmd "RATELIMIT_CHECK #2 (expect allowed)" '{"cmd":"RATELIMIT_CHECK","args":{"key":"req","limit":"3","window_ms":"60000","type":"fixed"}}'
  post_cmd "RATELIMIT_CHECK #3 (expect allowed)" '{"cmd":"RATELIMIT_CHECK","args":{"key":"req","limit":"3","window_ms":"60000","type":"fixed"}}'
  post_cmd "RATELIMIT_CHECK #4 (expect denied)" '{"cmd":"RATELIMIT_CHECK","args":{"key":"req","limit":"3","window_ms":"60000","type":"fixed"}}'
}

run_all_commands() {
  cmds_string
  cmds_set
  cmds_zset
  cmds_geo
  cmds_bloom
  cmds_cms
  cmds_pubsub
  cmds_ratelimit
}

usage() {
  echo "Usage: ./scripts/http-commands.sh [all|string|set|zset|geo|bloom|cms|pubsub|ratelimit]"
}

case "${1:-all}" in
  all)       ensure_server; run_all_commands ;;
  string)    ensure_server; cmds_string ;;
  set)       ensure_server; cmds_set ;;
  zset)      ensure_server; cmds_zset ;;
  geo)       ensure_server; cmds_geo ;;
  bloom)     ensure_server; cmds_bloom ;;
  cms)       ensure_server; cmds_cms ;;
  pubsub)    ensure_server; cmds_pubsub ;;
  ratelimit) ensure_server; cmds_ratelimit ;;
  -h|--help) usage ;;
  *)
    echo "Unknown target: $1"
    usage
    exit 2
    ;;
esac
