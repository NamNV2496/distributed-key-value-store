package shard

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/status"
)

func (m *Manager) localStatus() status.NodeStatus {
	topo := m.topo.Get()
	counts := topo.SlotCounts()

	m.mu.RLock()
	shards := make([]status.ShardStatus, 0, len(m.groups))
	for id, g := range m.groups {
		node := g.RaftNode()
		cfg := node.Configuration()
		voters := make([]string, 0, len(cfg.Voters))
		for nodeID := range cfg.Voters {
			voters = append(voters, nodeID)
		}
		sort.Strings(voters)
		learners := make([]string, 0, len(cfg.Learners))
		for nodeID := range cfg.Learners {
			learners = append(learners, nodeID)
		}
		sort.Strings(learners)

		shards = append(shards, status.ShardStatus{
			ShardID:     id,
			Role:        string(node.GetRole()),
			LeaderID:    node.GetLeaderID(),
			Term:        node.GetCurrentTerm(),
			CommitIndex: node.GetCommitIndex(),
			Slots:       counts[id],
			Members:     voters,
			Learners:    learners,
		})
	}
	m.mu.RUnlock()
	sort.Slice(shards, func(i, j int) bool { return shards[i].ShardID < shards[j].ShardID })

	return status.NodeStatus{
		NodeID:          m.nodeID,
		TopologyVersion: topo.Version,
		SlotCount:       topo.SlotCount,
		Shards:          shards,
		Migrations:      topo.Migrations,
	}
}

func (m *Manager) HandleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topo := m.topo.Get()
	probes := m.probeNodes(r.Context(), topo)

	m.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, status.ClusterStatus{
		ServedBy:    m.nodeID,
		GeneratedAt: time.Now().UnixMilli(),
		Topology:    status.Summarise(topo),
		Shards:      status.BuildShardViews(topo, probes),
		Nodes:       probes,
		Migrations:  topo.Migrations,
	})
}

func (m *Manager) probeNodes(ctx context.Context, topo *cluster.Topology) []status.NodeProbe {
	return status.Prober{
		Client: m.httpClient,
		Local:  m.nodeID,
		Self:   m.localStatus,
	}.Probe(ctx, cluster.NodeAddresses(topo))
}
