package shard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

func (m *Manager) Routes(mux *http.ServeMux) {

	mux.HandleFunc("/kv", m.HandleKV)

	mux.HandleFunc("/shards/{shard}/raft/vote", m.shardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleRequestVote
	}))
	mux.HandleFunc("/shards/{shard}/raft/append", m.shardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleAppendEntries
	}))
	mux.HandleFunc("/shards/{shard}/raft/command", m.shardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleCommand
	}))
	mux.HandleFunc("/shards/{shard}/status", m.shardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleStatus
	}))

	mux.HandleFunc("/cluster/topology", m.HandleTopology)
	mux.HandleFunc("/cluster/shards", m.HandleShards)
	mux.HandleFunc("/cluster/rebalance", m.HandleRebalance)
	mux.HandleFunc("/cluster/locate", m.HandleLocate)
	mux.HandleFunc("/cluster/status", m.HandleClusterStatus)

	mux.HandleFunc("/health", m.HandleHealth)
	mux.HandleFunc("/status", m.HandleStatus)

	mux.HandleFunc("/raft/vote", m.soleShardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleRequestVote
	}))
	mux.HandleFunc("/raft/append", m.soleShardHandler(func(s *redis.RaftRedisServer) http.HandlerFunc {
		return s.HandleAppendEntries
	}))
	mux.HandleFunc("/raft/command", m.HandleKV)
}

func (m *Manager) shardHandler(pick func(*redis.RaftRedisServer) http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shardID := r.PathValue("shard")
		g, ok := m.group(shardID)
		if !ok {
			m.setTopologyHeader(w)
			http.Error(w, fmt.Sprintf("shard %q is not hosted on %s", shardID, m.nodeID), http.StatusNotFound)
			return
		}
		m.setTopologyHeader(w)
		pick(g)(w, r)
	}
}

func (m *Manager) soleShardHandler(pick func(*redis.RaftRedisServer) http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.RLock()
		var only *redis.RaftRedisServer
		count := len(m.groups)
		for _, g := range m.groups {
			only = g
		}
		m.mu.RUnlock()

		if count != 1 {
			http.Error(w, fmt.Sprintf(
				"%s hosts %d shards; use /shards/{shard}%s", m.nodeID, count, r.URL.Path),
				http.StatusConflict)
			return
		}
		pick(only)(w, r)
	}
}

func (m *Manager) setTopologyHeader(w http.ResponseWriter) {
	w.Header().Set(routing.TopologyVersionHeader, strconv.FormatInt(m.topo.Get().Version, 10))
}

func (m *Manager) HandleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, routing.MaxBodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	var cmd redis.Command
	if err := json.Unmarshal(body, &cmd); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse command: %v", err), http.StatusBadRequest)
		return
	}
	if cmd.Cmd == "" {
		http.Error(w, "missing command", http.StatusBadRequest)
		return
	}

	topo := m.topo.Get()
	resp := routing.KVResponse{
		NodeID:          m.nodeID,
		Command:         cmd.Cmd,
		Slot:            -1,
		TopologyVersion: topo.Version,
	}

	// A node hosting the shard should serve the command itself rather than
	// hopping to a peer, so its keyless fallback prefers a local shard.
	dec, err := routing.Route(topo, &cmd, func(*cluster.Topology) (string, bool) { return m.anyLocalShard() })
	resp.Slot = dec.Slot
	resp.Shard = dec.ShardID
	if err != nil {
		m.refuseKV(w, resp, err)
		return
	}

	if !dec.Keyless && r.URL.Query().Get("redirect") != "" {
		resp.Status = "moved"
		resp.Moved = &routing.MovedInfo{Slot: dec.Slot, Shard: dec.ShardID, Nodes: topo.Shards[dec.ShardID].Members}
		m.writeKV(w, http.StatusTemporaryRedirect, resp)
		return
	}

	result, servedBy, err := m.shardCommand(r.Context(), dec.ShardID, &cmd)
	m.finish(w, resp, result, servedBy, err)
}

// refuseKV renders a routing refusal. Both /kv implementations funnel through a
// routing.Error so the status and the Retry-After hint stay in one place.
func (m *Manager) refuseKV(w http.ResponseWriter, resp routing.KVResponse, err error) {
	code := http.StatusServiceUnavailable
	var re *routing.Error
	if errors.As(err, &re) {
		code = re.Status4xx()
		if re.RetryAfter {
			w.Header().Set("Retry-After", "1")
		}
	}
	m.writeKV(w, code, resp.WithError(err.Error()))
}

func (m *Manager) finish(w http.ResponseWriter, resp routing.KVResponse, result any, servedBy string, err error) {
	resp.ServedBy = servedBy
	if err != nil {
		m.writeKV(w, http.StatusInternalServerError, resp.WithError(err.Error()))
		return
	}
	resp.Status = "success"
	resp.Result = result
	m.writeKV(w, http.StatusOK, resp)
}

func (m *Manager) writeKV(w http.ResponseWriter, code int, resp routing.KVResponse) {
	m.setTopologyHeader(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[%s] failed to write /kv response: %v", m.nodeID, err)
	}
}

func (m *Manager) anyLocalShard() (string, bool) {
	ids := m.LocalShards()
	if len(ids) == 0 {
		return "", false
	}
	sort.Strings(ids)
	return ids[0], true
}

func (m *Manager) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, map[string]any{
		"status":  "healthy",
		"node_id": m.nodeID,
		"shards":  m.LocalShards(),
	})
}

func (m *Manager) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, m.localStatus())
}
