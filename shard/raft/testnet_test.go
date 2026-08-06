package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type memNet struct {
	mu    sync.RWMutex
	nodes map[string]*RaftNode // address -> node
	down  map[string]bool      // address -> hard down (process killed)
	group map[string]int       // address -> partition group; peers talk only within a group
}

func newMemNet() *memNet {
	return &memNet{
		nodes: make(map[string]*RaftNode),
		down:  make(map[string]bool),
		group: make(map[string]int),
	}
}

func (n *memNet) partition(groups map[string]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = groups
}

func (n *memNet) register(addr string, node *RaftNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[addr] = node
}

// setDown makes addr unreachable (or reachable again) from every peer.
func (n *memNet) setDown(addr string, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[addr] = down
}

func (n *memNet) dial(from, to string) (*RaftNode, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.down[to] {
		return nil, errors.New("memnet: host down")
	}
	if n.group[from] != n.group[to] {
		return nil, errors.New("memnet: partitioned")
	}
	node, ok := n.nodes[to]
	if !ok {
		return nil, errors.New("memnet: no such host")
	}
	return node, nil
}

func (n *memNet) clientFactory(src string) func(addr string) RaftRPCClient {
	return func(addr string) RaftRPCClient {
		return &memClient{net: n, from: src, addr: addr}
	}
}

type memClient struct {
	net  *memNet
	from string
	addr string
}

func (c *memClient) RequestVote(ctx context.Context, args *RequestVoteArgs, _ ...interface{}) (*RequestVoteReply, error) {
	node, err := c.net.dial(c.from, c.addr)
	if err != nil {
		return nil, err
	}
	return node.RequestVote(ctx, args)
}

func (c *memClient) AppendEntries(ctx context.Context, args *AppendEntriesArgs, _ ...interface{}) (*AppendEntriesReply, error) {
	node, err := c.net.dial(c.from, c.addr)
	if err != nil {
		return nil, err
	}
	return node.AppendEntries(ctx, args)
}

// cluster is a set of running nodes wired together over a memNet.
type cluster struct {
	t     *testing.T
	net   *memNet
	nodes map[string]*RaftNode
	dir   string
	ids   []string
}

// newCluster starts n nodes that all know about each other.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()

	c := &cluster{
		t:     t,
		net:   newMemNet(),
		nodes: make(map[string]*RaftNode),
		dir:   t.TempDir(),
	}

	peers := make(map[string]string, n)
	for i := 1; i <= n; i++ {
		id := nodeName(i)
		peers[id] = id // address == id for the in-memory net
		c.ids = append(c.ids, id)
	}

	for _, id := range c.ids {
		node := c.start(id, peers)
		c.nodes[id] = node
	}
	for _, node := range c.nodes {
		node.Start()
	}
	t.Cleanup(c.stop)
	return c
}

func (c *cluster) start(id string, peers map[string]string) *RaftNode {
	c.t.Helper()
	node, err := NewRaftNode(Config{
		NodeID:           id,
		Peers:            peers,
		LogFile:          filepath.Join(c.dir, id+".wal"),
		StateFile:        filepath.Join(c.dir, id+".state"),
		ElectionTimeout:  60 * time.Millisecond,
		HeartbeatTimeout: 20 * time.Millisecond,
		NewClient:        c.net.clientFactory(id),
	})
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	c.net.register(id, node)

	// Drain the apply channel and acknowledge, standing in for a state machine.
	go func() {
		for entry := range node.ApplyChan() {
			node.MarkApplied(entry.Index)
		}
	}()
	return node
}

func (c *cluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

func nodeName(i int) string {
	return string(rune('a' + i - 1))
}

// leader returns the node currently claiming leadership, or nil.
func (c *cluster) leader() *RaftNode {
	for _, id := range c.ids {
		node := c.nodes[id]
		if c.net.isDown(id) {
			continue
		}
		if node.GetRole() == LeaderRole {
			return node
		}
	}
	return nil
}

func (n *memNet) isDown(addr string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.down[addr]
}

func (c *cluster) isolateFollower(leader *RaftNode) *RaftNode {
	c.t.Helper()
	var target string
	groups := make(map[string]int, len(c.ids))
	for _, id := range c.ids {
		if id == leader.nodeID {
			groups[id] = 2
			continue
		}
		if target == "" {
			target = id
			groups[id] = 1 // alone
			continue
		}
		groups[id] = 2
	}
	if target == "" {
		c.t.Fatal("cluster has no follower to isolate")
	}
	c.net.partition(groups)
	return c.nodes[target]
}

// majorityLeader returns the leader on the side of the partition that still
// holds a quorum, or nil if that side has none.
func (c *cluster) majorityLeader(excluded *RaftNode) *RaftNode {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if id == excluded.nodeID {
				continue
			}
			if c.nodes[id].GetRole() == LeaderRole {
				return c.nodes[id]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (c *cluster) awaitStable(timeout time.Duration) *RaftNode {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.stableLeader(); l != nil {
			time.Sleep(150 * time.Millisecond)
			if l2 := c.stableLeader(); l2 != nil && l2.nodeID == l.nodeID &&
				l2.GetCurrentTerm() == l.GetCurrentTerm() {
				return l2
			}
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not stabilise within %s", timeout)
	return nil
}

func (c *cluster) stableLeader() *RaftNode {
	var leader *RaftNode
	for _, id := range c.ids {
		node := c.nodes[id]
		if node.GetRole() == LeaderRole {
			if leader != nil {
				return nil // more than one claimant
			}
			leader = node
		}
	}
	if leader == nil {
		return nil
	}
	term := leader.GetCurrentTerm()
	for _, id := range c.ids {
		if c.nodes[id].GetCurrentTerm() != term {
			return nil
		}
	}
	return leader
}

// awaitLeader waits for exactly one reachable node to claim leadership.
func (c *cluster) awaitLeader(timeout time.Duration) *RaftNode {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func mustPropose(t *testing.T, node *RaftNode, cmd string, data []byte) int64 {
	t.Helper()
	idx, _, err := node.Propose(cmd, data)
	if err != nil {
		t.Fatalf("propose %s: %v", cmd, err)
	}
	return idx
}
