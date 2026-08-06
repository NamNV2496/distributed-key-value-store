package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/shard/raft/wal"
)

func TestMinorityNeverBecomesLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)

	isolated := c.isolateFollower(leader)

	// The lone follower must never reach leadership...
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if role := isolated.GetRole(); role == LeaderRole {
			t.Fatalf("isolated follower %s became leader with no quorum — split brain", isolated.nodeID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.majorityLeader(isolated) == nil {
		t.Fatal("majority side lost its leader; the partition did not isolate only one node")
	}
}

func TestIsolatedNodeDoesNotInflateTerm(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)
	isolated := c.isolateFollower(leader)
	startTerm := isolated.GetCurrentTerm()

	time.Sleep(1500 * time.Millisecond) // many election timeouts

	if got := isolated.GetCurrentTerm(); got != startTerm {
		t.Fatalf("isolated follower inflated its term from %d to %d despite pre-vote", startTerm, got)
	}
}

func TestLeaderFailoverElectsNewLeader(t *testing.T) {
	c := newCluster(t, 3)
	first := c.awaitLeader(3 * time.Second)

	// Let heartbeats reset the followers' timers many times over.
	time.Sleep(500 * time.Millisecond)

	c.net.setDown(first.nodeID, true)
	first.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if id == first.nodeID {
				continue
			}
			if c.nodes[id].GetRole() == LeaderRole {
				return // a survivor took over
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no new leader after the old one died — election timer is not firing")
}

func TestAtMostOneLeaderPerTerm(t *testing.T) {
	c := newCluster(t, 5)
	c.awaitLeader(3 * time.Second)

	seen := make(map[int64]string)
	deadline := time.Now().Add(3 * time.Second)
	var downed string

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.GetRole() != LeaderRole {
				continue
			}
			term := node.GetCurrentTerm()
			if prev, ok := seen[term]; ok && prev != node.nodeID {
				t.Fatalf("two leaders in term %d: %s and %s", term, prev, node.nodeID)
			}
			seen[term] = node.nodeID
		}

		// Churn: knock the current leader out, restore the previous one.
		if l := c.leader(); l != nil {
			if downed != "" {
				c.net.setDown(downed, false)
			}
			downed = l.nodeID
			c.net.setDown(downed, true)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)
	groups := map[string]int{leader.nodeID: 1}
	for _, id := range c.ids {
		if id != leader.nodeID {
			groups[id] = 2
		}
	}
	c.net.partition(groups)

	term := leader.GetCurrentTerm()
	reply, err := leader.AppendEntries(context.Background(), &AppendEntriesArgs{
		Term:         term + 5,
		LeaderId:     "someone-else",
		PrevLogIndex: -1,
		LeaderCommit: -1,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatalf("expected the higher-term AppendEntries to be accepted")
	}
	if role := leader.GetRole(); role != FollowerRole {
		t.Fatalf("leader stayed %s after seeing term %d — two leaders can coexist", role, term+5)
	}
	if got := leader.GetLeaderID(); got != "someone-else" {
		t.Fatalf("expected leaderId to follow the new leader, got %q", got)
	}
}

func TestAppendEntriesKeepsMatchingEntries(t *testing.T) {
	node := newStandaloneNode(t, "solo")
	defer node.Stop()

	entries := []*LogEntry{
		{Cmd: "SET", Term: 1, Index: 0, Data: []byte(`{"cmd":"SET"}`)},
		{Cmd: "SET", Term: 1, Index: 1, Data: []byte(`{"cmd":"SET"}`)},
		{Cmd: "SET", Term: 1, Index: 2, Data: []byte(`{"cmd":"SET"}`)},
	}
	args := &AppendEntriesArgs{Term: 1, LeaderId: "leader", PrevLogIndex: -1, Entries: entries, LeaderCommit: 2}
	if _, err := node.AppendEntries(context.Background(), args); err != nil {
		t.Fatalf("first AppendEntries: %v", err)
	}
	if got := node.log.Len(); got != 3 {
		t.Fatalf("expected 3 entries, got %d", got)
	}
	if _, err := node.AppendEntries(context.Background(), args); err != nil {
		t.Fatalf("replayed AppendEntries: %v", err)
	}
	if got := node.log.Len(); got != 3 {
		t.Fatalf("replayed RPC changed the log length to %d; entries were re-appended or truncated", got)
	}
	prefix := &AppendEntriesArgs{Term: 1, LeaderId: "leader", PrevLogIndex: -1, Entries: entries[:1], LeaderCommit: 2}
	if _, err := node.AppendEntries(context.Background(), prefix); err != nil {
		t.Fatalf("prefix AppendEntries: %v", err)
	}
	if got := node.log.Len(); got != 3 {
		t.Fatalf("prefix RPC truncated the log to %d entries", got)
	}
}

func TestAppendEntriesRefusesToTruncateCommitted(t *testing.T) {
	node := newStandaloneNode(t, "solo")
	defer node.Stop()

	base := []*LogEntry{
		{Cmd: "SET", Term: 1, Index: 0, Data: []byte(`{"cmd":"SET"}`)},
		{Cmd: "SET", Term: 1, Index: 1, Data: []byte(`{"cmd":"SET"}`)},
	}
	if _, err := node.AppendEntries(context.Background(), &AppendEntriesArgs{
		Term: 1, LeaderId: "leader", PrevLogIndex: -1, Entries: base, LeaderCommit: 1,
	}); err != nil {
		t.Fatalf("seed AppendEntries: %v", err)
	}
	if got := node.GetCommitIndex(); got != 1 {
		t.Fatalf("expected commitIndex 1, got %d", got)
	}
	conflicting := []*LogEntry{{Cmd: "DEL", Term: 2, Index: 1, Data: []byte(`{"cmd":"DEL"}`)}}
	_, err := node.AppendEntries(context.Background(), &AppendEntriesArgs{
		Term: 2, LeaderId: "leader", PrevLogIndex: 0, PrevLogTerm: 1, Entries: conflicting, LeaderCommit: 1,
	})
	if err == nil {
		t.Fatal("expected an error when asked to overwrite a committed entry")
	}
}

func TestReadIndexRejectsNonLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitLeader(3 * time.Second)

	for _, id := range c.ids {
		node := c.nodes[id]
		if node == leader {
			continue
		}
		if _, err := node.ReadIndex(context.Background()); err == nil {
			t.Fatalf("follower %s served a read index", id)
		}
	}
}

func TestPartitionedLeaderStopsServingReads(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)

	groups := map[string]int{leader.nodeID: 1}
	for _, id := range c.ids {
		if id != leader.nodeID {
			groups[id] = 2
		}
	}
	c.net.partition(groups)
	time.Sleep(300 * time.Millisecond) // outlive the leader lease

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := leader.ReadIndex(ctx); err == nil {
		t.Fatal("partitioned leader served a read index without quorum confirmation")
	}
}

func TestRestartReplaysCommittedEntries(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "solo.wal")
	stateFile := filepath.Join(dir, "solo.state")

	newNode := func() *RaftNode {
		node, err := NewRaftNode(Config{
			NodeID: "solo", Peers: map[string]string{}, LogFile: logFile, StateFile: stateFile,
		})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		return node
	}

	node := newNode()
	node.Start()

	applied := make(chan wal.LogEntry, 16)
	go func() {
		for e := range node.ApplyChan() {
			node.MarkApplied(e.Index)
			applied <- e
		}
	}()

	waitRole(t, node, LeaderRole, 2*time.Second)
	idx := mustPropose(t, node, "SET", []byte(`{"cmd":"SET","args":{"key":"k","value":"v"}}`))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := node.WaitApplied(ctx, idx); err != nil {
		t.Fatalf("wait applied: %v", err)
	}
	node.Stop()

	// Restart against the same files.
	restarted := newNode()
	restarted.Start()
	defer restarted.Stop()

	replayed := make(chan wal.LogEntry, 16)
	go func() {
		for e := range restarted.ApplyChan() {
			restarted.MarkApplied(e.Index)
			replayed <- e
		}
	}()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-replayed:
			if e.Cmd == "SET" {
				return // the write came back out of the log
			}
		case <-deadline:
			t.Fatal("restarted node never replayed the committed SET — data silently lost")
		}
	}
}

func newStandaloneNode(t *testing.T, id string) *RaftNode {
	t.Helper()
	dir := t.TempDir()
	node, err := NewRaftNode(Config{
		NodeID:    id,
		Peers:     map[string]string{id: id, "other": "other"}, // a peer so it is not single-node
		LogFile:   filepath.Join(dir, id+".wal"),
		StateFile: filepath.Join(dir, id+".state"),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return node
}

func waitRole(t *testing.T, node *RaftNode, want NodeRole, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.GetRole() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s never reached role %s (still %s)", node.nodeID, want, node.GetRole())
}
