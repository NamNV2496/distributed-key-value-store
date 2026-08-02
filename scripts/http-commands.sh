#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-5001}"
BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT}}"
TIMEOUT="${TIMEOUT:-20}"
SERVER_LOG="${SERVER_LOG:-/tmp/go-redis-raft-http.log}"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

ensure_server() {
  if curl -sS --max-time 2 "$BASE_URL/health" >/dev/null 2>&1; then
    return 0
  fi

  echo "Starting local server on $BASE_URL"
  NODE=node1 PORT="$PORT" THREADS=1 go run . redis >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!

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

run_all_commands() {
  post_cmd "PING" '{"cmd":"PING"}'
  post_cmd "SET" '{"cmd":"SET","args":{"key":"demo","value":"hello"}}'
  post_cmd "GET" '{"cmd":"GET","args":{"key":"demo"}}'
  post_cmd "TTL" '{"cmd":"TTL","args":{"key":"demo"}}'
  post_cmd "DEL" '{"cmd":"DEL","args":{"key":"demo"}}'
  post_cmd "EXPIRE" '{"cmd":"EXPIRE","args":{"key":"demo","ttl":"120"}}'
  post_cmd "INCR" '{"cmd":"INCR","args":{"key":"counter"}}'

  post_cmd "SADD" '{"cmd":"SADD","args":{"key":"myset","value":"alice"}}'
  post_cmd "SCARD" '{"cmd":"SCARD","args":{"key":"myset"}}'
  post_cmd "SISMEMBER" '{"cmd":"SISMEMBER","args":{"key":"myset","value":"alice"}}'
  post_cmd "SMISMEMBER" '{"cmd":"SMISMEMBER","args":{"key":"myset","value":"alice"}}'
  post_cmd "SMEMBERS" '{"cmd":"SMEMBERS","args":{"key":"myset"}}'

  post_cmd "ZADD" '{"cmd":"ZADD","args":{"key":"board","score":"100","member":"alice"}}'
  post_cmd "ZRANK" '{"cmd":"ZRANK","args":{"key":"board","member":"alice"}}'
  post_cmd "ZREM" '{"cmd":"ZREM","args":{"key":"board","member":"alice"}}'
  post_cmd "ZSCORE" '{"cmd":"ZSCORE","args":{"key":"board","member":"alice"}}'
  post_cmd "ZCARD" '{"cmd":"ZCARD","args":{"key":"board"}}'

  post_cmd "GEOADD" '{"cmd":"GEOADD","args":{"key":"cities","longitude":"13.361389","latitude":"38.115556","member":"Palermo"}}'
  post_cmd "GEODIST" '{"cmd":"GEODIST","args":{"key":"cities","member1":"Palermo","member2":"Catania","unit":"km"}}'
  post_cmd "GEOHASH" '{"cmd":"GEOHASH","args":{"key":"cities","m1":"Palermo","m2":"Catania"}}'
  post_cmd "GEOPOS" '{"cmd":"GEOPOS","args":{"key":"cities","m1":"Palermo"}}'

  post_cmd "BF_RESERVE" '{"cmd":"BF_RESERVE","args":{"key":"bf","errRate":"0.01","capacity":"10000"}}'
  post_cmd "BF_INFO" '{"cmd":"BF_INFO","args":{"key":"bf"}}'
  post_cmd "BF_MADD" '{"cmd":"BF_MADD","args":{"key":"bf","1":"foo","2":"bar"}}'
  post_cmd "BF_EXISTS" '{"cmd":"BF_EXISTS","args":{"key":"bf","item":"foo"}}'
  post_cmd "BF_MEXISTS" '{"cmd":"BF_MEXISTS","args":{"key":"bf","1":"foo","2":"missing"}}'

  post_cmd "CMS_INITBYDIM" '{"cmd":"CMS_INITBYDIM","args":{"key":"cms","width":"2000","height":"7"}}'
  post_cmd "CMS_QUERY" '{"cmd":"CMS_QUERY","args":{"key":"cms"}}'

  post_cmd "PUBLISH" '{"cmd":"PUBLISH","args":{"channel":"abc123","message":"hello"}}'
  post_cmd "SUBSCRIBE" '{"cmd":"SUBSCRIBE","args":{"channel":"abc123"}}'
  post_cmd "UNSUBSCRIBE" '{"cmd":"UNSUBSCRIBE","args":{"channel":"abc123"}}'
  post_cmd "PSUBSCRIBE" '{"cmd":"PSUBSCRIBE","args":{"channel":"abc*"}}'
  post_cmd "PUNSUBSCRIBE" '{"cmd":"PUNSUBSCRIBE","args":{"channel":"abc*"}}'
}

ensure_server

case "${1:-all}" in
  all)
    run_all_commands
    ;;
  set)
    post_cmd "SET" '{"cmd":"SET","args":{"key":"demo","value":"hello"}}'
    post_cmd "GET" '{"cmd":"GET","args":{"key":"demo"}}'
    ;;
  pubsub)
    post_cmd "PUBLISH" '{"cmd":"PUBLISH","args":{"channel":"abc123","message":"hello"}}'
    post_cmd "SUBSCRIBE" '{"cmd":"SUBSCRIBE","args":{"channel":"abc123"}}'
    post_cmd "PSUBSCRIBE" '{"cmd":"PSUBSCRIBE","args":{"channel":"abc*"}}'
    ;;
  ratelimit)
    post_cmd "RATELIMIT_INIT" '{"cmd":"RATELIMIT_INIT","args":{"key":"req","limit":"3","window":"60"}}'
    post_cmd "RATELIMIT_CHECK" '{"cmd":"RATELIMIT_CHECK","args":{"key":"req"}}'
    ;;
  *)
    echo "Unknown target: $1"
    echo "Usage: ./scripts/http-commands.sh [all|set|pubsub|ratelimit]"
    exit 2
    ;;
esac
