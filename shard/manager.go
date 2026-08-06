package shard

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/raft"
	"github.com/namnv2496/go-redis-raft/shard/redis"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

type Config struct {
	NodeID           string
	Advertise        string
	DataDir          string
	EvictStrategy    int
	Bootstrap        *cluster.Topology
	TopologyPath     string
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
}

type Manager struct {
	nodeID           string
	advertise        string
	dataDir          string
	evict            int
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	topo             *cluster.Store
	mu               sync.RWMutex
	groups           map[string]*redis.RaftRedisServer
	closed           bool
	rebalancing      sync.Mutex
	httpClient       *http.Client
	stopCh           chan struct{}
	peer             *routing.PeerClient
}

func New(cfg Config) (*Manager, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("shard: NodeID is required")
	}

	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, fmt.Errorf("shard: create data dir: %w", err)
		}
	}

	topo, err := cluster.LoadTopology(cfg.TopologyPath)
	if err != nil {
		return nil, fmt.Errorf("shard: load topology: %w", err)
	}
	if topo == nil {
		topo = cfg.Bootstrap
	} else if cfg.Bootstrap != nil && cfg.Bootstrap.Version > topo.Version {
		topo = cfg.Bootstrap
	}
	if topo == nil {
		return nil, fmt.Errorf("shard: no topology to start from")
	}
	if err := topo.Validate(); err != nil {
		return nil, fmt.Errorf("shard: invalid topology: %w", err)
	}

	m := &Manager{
		nodeID:           cfg.NodeID,
		advertise:        cfg.Advertise,
		dataDir:          cfg.DataDir,
		evict:            cfg.EvictStrategy,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		topo:             cluster.NewStore(topo, cfg.TopologyPath),
		groups:           make(map[string]*redis.RaftRedisServer),
		httpClient:       &http.Client{Timeout: 15 * time.Second},
		stopCh:           make(chan struct{}),
	}
	m.peer = routing.NewPeerClient(m.nodeID, m.topo, m.httpClient, m.installTopology)
	if err := m.reconcile(nil, topo); err != nil {
		return nil, err
	}
	go m.membershipLoop()
	return m, nil
}

func (m *Manager) NodeID() string { return m.nodeID }

func (m *Manager) Topology() *cluster.Topology { return m.topo.Get() }

func (m *Manager) LocalShards() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.groups))
	for id := range m.groups {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manager) group(shardID string) (*redis.RaftRedisServer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.groups[shardID]
	return g, ok
}

func (m *Manager) installTopology(next *cluster.Topology) (bool, error) {
	prev := m.topo.Get()
	installed, err := m.topo.Install(next)
	if err != nil || !installed {
		return installed, err
	}
	return true, m.reconcile(prev, m.topo.Get())
}

func (m *Manager) updateTopology(fn func(*cluster.Topology) (*cluster.Topology, error)) (*cluster.Topology, error) {
	prev := m.topo.Get()
	next, err := m.topo.Update(fn)
	if err != nil {
		return nil, err
	}
	return next, m.reconcile(prev, next)
}

func (m *Manager) reconcile(prev, topo *cluster.Topology) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}

	wanted := make(map[string]*cluster.Shard)
	for _, s := range topo.ShardsForNode(m.nodeID) {
		wanted[s.ID] = s
	}

	for id, server := range m.groups {
		if _, keep := wanted[id]; keep {
			continue
		}
		if _, shardLives := topo.Shards[id]; shardLives {
			if _, stillInGroup := server.RaftNode().Configuration().Members()[m.nodeID]; stillInGroup {
				continue
			}
		}
		log.Printf("[%s] shard %s is no longer assigned here; stopping its Raft group", m.nodeID, id)
		server.Stop()
		delete(m.groups, id)
	}

	for id, shard := range wanted {
		if _, running := m.groups[id]; running {
			continue
		}
		joining := joiningExistingShard(prev, id, m.nodeID)
		server, err := m.startGroup(shard, joining)
		if err != nil {
			return fmt.Errorf("shard %s: %w", id, err)
		}
		m.groups[id] = server
		if joining {
			log.Printf("[%s] joining existing shard %s as a learner (members: %v)", m.nodeID, id, shard.NodeIDs())
		} else {
			log.Printf("[%s] started Raft group for shard %s (members: %v)", m.nodeID, id, shard.NodeIDs())
		}
	}
	return nil
}

func joiningExistingShard(prev *cluster.Topology, shardID, nodeID string) bool {
	if prev == nil {
		return false
	}
	before, existed := prev.Shards[shardID]
	if !existed {
		return false
	}
	_, wasMember := before.Members[nodeID]
	return !wasMember
}

func (m *Manager) startGroup(shard *cluster.Shard, joining bool) (*redis.RaftRedisServer, error) {

	shardPeers := shardPeerURLs(shard)

	node, err := raft.NewRaftNode(raft.Config{
		NodeID:           m.nodeID,
		Peers:            shardPeers,
		JoinAsLearner:    joining,
		LogFile:          m.shardFile(shard.ID, "log"),
		StateFile:        m.shardFile(shard.ID, "state"),
		ElectionTimeout:  m.electionTimeout,
		HeartbeatTimeout: m.heartbeatTimeout,
		OnStateChange: func(role raft.NodeRole) {
			log.Printf("[%s/%s] role changed to %s", m.nodeID, shard.ID, role)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create raft node: %w", err)
	}

	store := redis.NewRedisStoreWithEviction(node, m.evict)

	server, err := redis.NewShardRedisServer(shard.ID, m.nodeID, node, store, shardPeers)
	if err != nil {
		node.Stop()
		return nil, fmt.Errorf("create shard server: %w", err)
	}
	server.StartExecutionLoop()
	go store.RunApplyLoop()
	return server, nil
}

func (m *Manager) shardFile(shardID, ext string) string {
	name := fmt.Sprintf(".raft-%s-%s.%s", m.nodeID, shardID, ext)
	if m.dataDir == "" {
		return name
	}
	return filepath.Join(m.dataDir, name)
}

func EvictStrategyFromPolicy(policy string) int {
	switch policy {
	case "lru":
		return data_structure.EvictLRU
	case "lfu":
		return data_structure.EvictLFU
	default:
		return data_structure.EvictFirst
	}
}

func (m *Manager) WaitShardLeader(ctx context.Context, shardID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g, ok := m.group(shardID); ok {
			if g.RaftNode().GetLeaderID() != "" {
				return nil
			}
		} else {
			leader, err := m.remoteShardLeader(ctx, shardID)
			if err != nil {
				lastErr = err
			} else if leader != "" {
				return nil
			}
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if lastErr != nil {
		return fmt.Errorf("shard %s has no leader after %s: %w", shardID, timeout, lastErr)
	}
	return fmt.Errorf("shard %s has no leader after %s", shardID, timeout)
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.stopCh)
	for id, server := range m.groups {
		server.Stop()
		delete(m.groups, id)
	}
}
