// Package status is the shared vocabulary for "what is this cluster doing right
// now": what one node reports about itself, and how a caller folds many of those
// answers into one view.
//
// It is its own package because all three process types assemble the same
// document — a node from its own topology, a proxy from the one it routes by,
// and the standalone dashboard from whichever seed answered. Keeping one copy
// means the dashboard renders identical JSON whichever of them served it.
package status

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
)

// ProbeTimeout bounds the fan-out behind /cluster/status: a node that has gone
// away should show up as unreachable quickly rather than stall the page.
const ProbeTimeout = 3 * time.Second

// ShardStatus is what one node knows about one Raft group it hosts.
type ShardStatus struct {
	ShardID     string `json:"shard_id"`
	Role        string `json:"role"`
	LeaderID    string `json:"leader_id"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
	Slots       int    `json:"slots"`

	Members  []string `json:"members,omitempty"`
	Learners []string `json:"learners,omitempty"`
}

// NodeStatus is the payload of /status: everything a single node can answer
// about itself without talking to anyone else.
type NodeStatus struct {
	NodeID          string              `json:"node_id"`
	TopologyVersion int64               `json:"topology_version"`
	SlotCount       int                 `json:"slot_count"`
	Shards          []ShardStatus       `json:"shards"`
	Migrations      []cluster.Migration `json:"migrations,omitempty"`
}

// NodeProbe is one row of the node table: the reachability of a node plus the
// status it reported back.
type NodeProbe struct {
	NodeID          string        `json:"node_id"`
	Address         string        `json:"address"`
	Local           bool          `json:"local"`
	Reachable       bool          `json:"reachable"`
	Error           string        `json:"error,omitempty"`
	LatencyMS       int64         `json:"latency_ms"`
	TopologyVersion int64         `json:"topology_version"`
	Shards          []ShardStatus `json:"shards"`
}

type MemberView struct {
	NodeID      string `json:"node_id"`
	Address     string `json:"address"`
	Role        string `json:"role"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
	Reachable   bool   `json:"reachable"`
	Local       bool   `json:"local"`
	Error       string `json:"error,omitempty"`
}

type ShardView struct {
	ID       string       `json:"id"`
	Slots    int          `json:"slots"`
	Share    float64      `json:"share"`
	LeaderID string       `json:"leader_id"`
	Term     int64        `json:"term"`
	Healthy  int          `json:"healthy_members"`
	Members  []MemberView `json:"members"`
}

type TopologySummary struct {
	Version   int64   `json:"version"`
	SlotCount int     `json:"slot_count"`
	VNodes    int     `json:"vnodes"`
	Epsilon   float64 `json:"epsilon"`
}

// ClusterStatus is the document behind /cluster/status.
type ClusterStatus struct {
	ServedBy string `json:"served_by"`

	// Source names the node a standalone dashboard read the topology from; it
	// is empty when a node assembled the view from its own topology.
	Source string `json:"source,omitempty"`

	GeneratedAt int64               `json:"generated_at"`
	Topology    TopologySummary     `json:"topology"`
	Shards      []ShardView         `json:"shards"`
	Nodes       []NodeProbe         `json:"nodes"`
	Migrations  []cluster.Migration `json:"migrations,omitempty"`
}

// Summarise folds a topology into the header of a status document.
func Summarise(topo *cluster.Topology) TopologySummary {
	return TopologySummary{
		Version:   topo.Version,
		SlotCount: topo.SlotCount,
		VNodes:    topo.VNodes,
		Epsilon:   topo.Epsilon,
	}
}

// Prober asks a set of nodes for their status in parallel. Local and Self are
// set when the caller is itself one of those nodes; a proxy or the standalone
// dashboard leaves them empty and asks everyone.
type Prober struct {
	Client *http.Client
	Local  string
	Self   func() NodeStatus
}

func (p Prober) Probe(ctx context.Context, addrs map[string]string) []NodeProbe {
	ids := make([]string, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	probes := make([]NodeProbe, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		// Shards starts empty rather than nil so an unreachable node still
		// serialises as [] and the UI can iterate it without a guard.
		probes[i] = NodeProbe{NodeID: id, Address: addrs[id], Local: id == p.Local, Shards: []ShardStatus{}}
		if id == p.Local && p.Self != nil {
			status := p.Self()
			probes[i].Reachable = true
			probes[i].TopologyVersion = status.TopologyVersion
			probes[i].Shards = status.Shards
			continue
		}

		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			started := time.Now()
			status, err := fetchNodeStatus(ctx, p.Client, addr)
			probes[i].LatencyMS = time.Since(started).Milliseconds()
			if err != nil {
				probes[i].Error = err.Error()
				return
			}
			probes[i].Reachable = true
			probes[i].TopologyVersion = status.TopologyVersion
			probes[i].Shards = status.Shards
		}(i, addrs[id])
	}
	wg.Wait()
	return probes
}

func fetchNodeStatus(ctx context.Context, client *http.Client, baseURL string) (*NodeStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errStatus(resp.Status)
	}
	var status NodeStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, routing.MaxBodyBytes)).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

type errStatus string

func (e errStatus) Error() string { return string(e) }

// BuildShardViews joins the slot table (what the topology says) with the probe
// results (what each node reports), so a member that owns slots but never
// answered still shows up — as unreachable rather than as missing.
func BuildShardViews(topo *cluster.Topology, probes []NodeProbe) []ShardView {
	reported := make(map[string]map[string]ShardStatus, len(probes))
	byNode := make(map[string]*NodeProbe, len(probes))
	for i := range probes {
		byNode[probes[i].NodeID] = &probes[i]
		for _, s := range probes[i].Shards {
			if reported[s.ShardID] == nil {
				reported[s.ShardID] = make(map[string]ShardStatus)
			}
			reported[s.ShardID][probes[i].NodeID] = s
		}
	}

	counts := topo.SlotCounts()
	views := make([]ShardView, 0, len(topo.Shards))
	for _, shardID := range topo.ShardIDs() {
		shard := topo.Shards[shardID]
		view := ShardView{ID: shardID, Slots: counts[shardID]}
		if topo.SlotCount > 0 {
			view.Share = float64(view.Slots) / float64(topo.SlotCount)
		}

		for _, nodeID := range shard.NodeIDs() {
			member := MemberView{NodeID: nodeID, Address: shard.Members[nodeID]}
			if probe, ok := byNode[nodeID]; ok {
				member.Local = probe.Local
				member.Error = probe.Error
			}
			if status, ok := reported[shardID][nodeID]; ok {
				member.Reachable = true
				member.Role = status.Role
				member.Term = status.Term
				member.CommitIndex = status.CommitIndex
				view.Healthy++
				if status.Term > view.Term {
					view.Term = status.Term
				}
				if status.LeaderID != "" {
					view.LeaderID = status.LeaderID
				}
			}
			view.Members = append(view.Members, member)
		}
		views = append(views, view)
	}
	return views
}
