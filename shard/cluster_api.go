package shard

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
)

func (m *Manager) HandleTopology(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		topo := m.topo.Get()
		m.setTopologyHeader(w)
		if r.URL.Query().Get("summary") != "" {
			routing.WriteJSON(w, http.StatusOK, map[string]any{
				"version":         topo.Version,
				"slot_count":      topo.SlotCount,
				"shards":          topo.Shards,
				"slots_per_shard": topo.SlotCounts(),
				"migrations":      topo.Migrations,
			})
			return
		}
		routing.WriteJSON(w, http.StatusOK, topo)

	case http.MethodPost:
		var next cluster.Topology
		if err := json.NewDecoder(io.LimitReader(r.Body, routing.MaxBodyBytes)).Decode(&next); err != nil {
			http.Error(w, fmt.Sprintf("failed to parse topology: %v", err), http.StatusBadRequest)
			return
		}
		installed, err := m.installTopology(&next)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to install topology: %v", err), http.StatusBadRequest)
			return
		}
		current := m.topo.Get()
		m.setTopologyHeader(w)
		routing.WriteJSON(w, http.StatusOK, map[string]any{
			"installed": installed,
			"version":   current.Version,
			"shards":    current.ShardIDs(),
			"hosting":   m.LocalShards(),
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *Manager) HandleLocate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing 'key' query parameter", http.StatusBadRequest)
		return
	}
	topo := m.topo.Get()
	slot, shard := topo.Locate(key)

	payload := map[string]any{
		"key":              key,
		"hash_tag":         cluster.HashTag(key),
		"slot":             slot,
		"slot_count":       topo.SlotCount,
		"topology_version": topo.Version,
	}
	if shard != nil {
		payload["shard"] = shard.ID
		payload["nodes"] = shard.Members
	}
	if mig := topo.MigrationFor(slot); mig != nil {
		payload["migrating"] = mig
	}
	m.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, payload)
}

type shardChangeRequest struct {
	Action  string            `json:"action"`
	ShardID string            `json:"shard"`
	Members map[string]string `json:"members"`
	DryRun  bool              `json:"dry_run"`

	MaxSlots int `json:"max_slots"`
}

func (m *Manager) HandleShards(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		topo := m.topo.Get()
		m.setTopologyHeader(w)
		routing.WriteJSON(w, http.StatusOK, map[string]any{
			"version":         topo.Version,
			"shards":          topo.Shards,
			"slots_per_shard": topo.SlotCounts(),
			"hosting":         m.LocalShards(),
		})

	case http.MethodPost:
		var req shardChangeRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, routing.MaxBodyBytes)).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("failed to parse request: %v", err), http.StatusBadRequest)
			return
		}

		topo := m.topo.Get()
		var (
			shards []*cluster.Shard
			err    error
		)
		switch req.Action {
		case "add":
			shards, err = cluster.AddShard(topo, &cluster.Shard{ID: req.ShardID, Members: req.Members})
		case "remove":
			shards, err = cluster.RemoveShard(topo, req.ShardID)
		default:
			http.Error(w, `"action" must be "add" or "remove"`, http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		report, err := m.Rebalance(r.Context(), RebalanceRequest{
			Shards:   shards,
			DryRun:   req.DryRun,
			MaxSlots: req.MaxSlots,
		})
		if err != nil {
			m.setTopologyHeader(w)
			routing.WriteJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "report": report})
			return
		}
		m.setTopologyHeader(w)
		routing.WriteJSON(w, http.StatusOK, report)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type rebalanceRequestBody struct {
	Shards   []*cluster.Shard `json:"shards,omitempty"`
	DryRun   bool             `json:"dry_run"`
	MaxSlots int              `json:"max_slots"`
}

func (m *Manager) HandleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req rebalanceRequestBody
	if r.Body != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, routing.MaxBodyBytes)).Decode(&req); err != nil && err != io.EOF {
			http.Error(w, fmt.Sprintf("failed to parse request: %v", err), http.StatusBadRequest)
			return
		}
	}

	if r.URL.Query().Get("dry_run") != "" {
		req.DryRun = true
	}

	shards := req.Shards
	if len(shards) == 0 {
		topo := m.topo.Get()
		for _, id := range topo.ShardIDs() {
			shards = append(shards, topo.Shards[id].Clone())
		}
	}

	report, err := m.Rebalance(r.Context(), RebalanceRequest{
		Shards:   shards,
		DryRun:   req.DryRun,
		MaxSlots: req.MaxSlots,
	})
	if err != nil {
		m.setTopologyHeader(w)
		routing.WriteJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "report": report})
		return
	}
	m.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, report)
}
