package redis

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/cmd"
)

type raftOptions struct {
	NodeID    string
	Advertise string
	Peers     map[string]string
	DataDir   string
}

func raftOptionsFromEnv() raftOptions {
	nodeID := cmd.Get("NODE", "node1")
	advertise := strings.TrimRight(cmd.Get("ADVERTISE", fmt.Sprintf("http://%s:5000", nodeID)), "/")

	peers := parsePeers(cmd.Get("PEERS", ""))
	peers[nodeID] = advertise
	dataDir := cmd.Get("DATA_DIR", "")
	if dataDir == "" {
		if logFile := cmd.Get("LOGFILE", ""); logFile != "" {
			dataDir = filepath.Dir(logFile)
		}
	}

	return raftOptions{NodeID: nodeID, Advertise: advertise, Peers: peers, DataDir: dataDir}
}

func parsePeers(spec string) map[string]string {
	peers := make(map[string]string)
	for peer := range strings.SplitSeq(spec, ",") {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		nodeID, addr, ok := strings.Cut(peer, "=")
		if !ok {
			continue
		}
		peers[nodeID] = strings.TrimRight(addr, "/")
	}
	return peers
}

type clusterOptions struct {
	ShardID   string
	Shards    []*cluster.Shard
	SlotCount int
	VNodes    int
}

func clusterOptionsFromEnv() (clusterOptions, error) {
	opts := clusterOptions{
		ShardID:   cmd.Get("SHARD_ID", "shard-0"),
		SlotCount: cluster.DefaultSlotCount,
		VNodes:    cluster.DefaultVNodes,
	}

	var err error
	switch {
	case cmd.Get("SHARDS_FILE", "") != "":
		opts.Shards, err = loadShardsFile(cmd.Get("SHARDS_FILE", ""))
	case cmd.Get("SHARDS", "") != "":
		opts.Shards, err = parseShardSpec(cmd.Get("SHARDS", ""))
	}
	if err != nil {
		return clusterOptions{}, err
	}

	if raw := cmd.Get("SLOTS", ""); raw != "" {
		opts.SlotCount, err = strconv.Atoi(raw)
		if err != nil || opts.SlotCount <= 0 {
			return clusterOptions{}, fmt.Errorf("SLOTS must be a positive integer, got %q", raw)
		}
	}
	if raw := cmd.Get("SHARD_VNODES", ""); raw != "" {
		opts.VNodes, err = strconv.Atoi(raw)
		if err != nil || opts.VNodes <= 0 {
			return clusterOptions{}, fmt.Errorf("SHARD_VNODES must be a positive integer, got %q", raw)
		}
	}
	return opts, nil
}

func (o clusterOptions) bootstrap(peers map[string]string) (*cluster.Topology, error) {
	shards := o.Shards
	if len(shards) == 0 {
		shards = []*cluster.Shard{{ID: o.ShardID, Members: peers}}
	}
	return cluster.NewTopology(shards, o.SlotCount, o.VNodes, cluster.DefaultEpsilon)
}

func parseShardSpec(spec string) ([]*cluster.Shard, error) {
	var shards []*cluster.Shard
	for _, group := range strings.Split(spec, ";") {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		id, memberList, found := strings.Cut(group, ":")
		id = strings.TrimSpace(id)
		if !found || id == "" {
			return nil, fmt.Errorf("shard %q must be written as <shard-id>:<node>=<url>,...", group)
		}
		shard := &cluster.Shard{ID: id, Members: map[string]string{}}
		for _, member := range strings.Split(memberList, ",") {
			member = strings.TrimSpace(member)
			if member == "" {
				continue
			}
			nodeID, addr, ok := strings.Cut(member, "=")
			if !ok || nodeID == "" || addr == "" {
				return nil, fmt.Errorf("shard %q: member %q must be written as <node-id>=<url>", id, member)
			}
			shard.Members[strings.TrimSpace(nodeID)] = strings.TrimRight(strings.TrimSpace(addr), "/")
		}
		if len(shard.Members) == 0 {
			return nil, fmt.Errorf("shard %q has no members", id)
		}
		shards = append(shards, shard)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("SHARDS is set but describes no shard")
	}
	return shards, nil
}

func loadShardsFile(path string) ([]*cluster.Shard, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var topo cluster.Topology
	if err := json.Unmarshal(data, &topo); err == nil && len(topo.Shards) > 0 {
		out := make([]*cluster.Shard, 0, len(topo.Shards))
		for _, id := range topo.ShardIDs() {
			out = append(out, topo.Shards[id].Clone())
		}
		return out, nil
	}
	var shards []*cluster.Shard
	if err := json.Unmarshal(data, &shards); err != nil {
		return nil, fmt.Errorf("%s is neither a topology nor a shard list: %w", path, err)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("%s describes no shard", path)
	}
	return shards, nil
}

type shardOptions struct {
	Port        int
	EvictPolicy string // noeviction | lru | lfu
	Threads     int
}

func shardOptionsFromEnv() (shardOptions, error) {
	port, err := cmd.Port("PORT", 5000)
	if err != nil {
		return shardOptions{}, err
	}
	return shardOptions{
		Port:        port,
		EvictPolicy: cmd.Get("EVICT_POLICY", "lru"),
		Threads:     threadCount(),
	}, nil
}

func threadCount() int {
	value, err := strconv.Atoi(cmd.Get("THREADS", "3"))
	if err != nil || value <= 0 {
		return 4
	}
	return value
}
