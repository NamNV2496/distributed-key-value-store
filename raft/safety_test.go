package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/wal"
)

// A follower cut off from the quorum must never promote itself.
//
// This is the split-brain regression. The old health-check worker promoted a
// node to leader as soon as every peer looked unreachable, so a 1-of-3
// partition produced a second leader and two divergent halves of the cluster.
//
// The isolated node must be a FOLLOWER. Isolating the current leader proves
// nothing: Raft leaders do not step down just because they lost contact — they
// stay in LeaderRole (unable to commit anything) until they learn of a higher
// term. That case is covered by TestPartitionedLeaderStopsServingReads.
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

	// ...while the majority side carries on, which confirms the partition
	// actually took effect rather than freezing the whole cluster.
	if c.majorityLeader(isolated) == nil {
		t.Fatal("majority side lost its leader; the partition did not isolate only one node")
	}
}

// Pre-vote must stop an isolated node from inflating its term, so that when it
// rejoins it does not force the healthy leader to step down.
func TestIsolatedNodeDoesNotInflateTerm(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)

	// A follower again: a leader never campaigns, so isolating one would make
	// this test pass without exercising pre-vote at all.
	isolated := c.isolateFollower(leader)
	startTerm := isolated.GetCurrentTerm()

	time.Sleep(1500 * time.Millisecond) // many election timeouts

	if got := isolated.GetCurrentTerm(); got != startTerm {
		t.Fatalf("isolated follower inflated its term from %d to %d despite pre-vote", startTerm, got)
	}
}

// Killing the leader must produce a new one. The election timer has to keep
// firing after it has been reset thousands of times by heartbeats — the old
// implementation replaced the timer object on every reset while run() was
// parked on the previous one, so timeouts stopped happening entirely.
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

// Across repeated failovers, two nodes must never claim the same term.
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

// A leader that receives AppendEntries from a newer term must step down.
// It used to adopt the higher term but stay in LeaderRole, leaving two live
// leaders serving clients.
func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.awaitStable(5 * time.Second)

	// Cut the leader off from its peers before poking it. Otherwise the
	// survivors notice the missing heartbeats, campaign, and their RequestVote
	// clears leaderId between the call below and the assertion — leaderId is
	// only a hint, and what this test is really about is the role transition.
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

// AppendEntries must not delete entries that match the leader's log. The old
// code truncated unconditionally, so a duplicated or reordered RPC could drop
// entries the node had already committed.
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

	// Replay the identical RPC — a retry the leader might legitimately send.
	if _, err := node.AppendEntries(context.Background(), args); err != nil {
		t.Fatalf("replayed AppendEntries: %v", err)
	}
	if got := node.log.Len(); got != 3 {
		t.Fatalf("replayed RPC changed the log length to %d; entries were re-appended or truncated", got)
	}

	// A shorter prefix must also leave the tail intact.
	prefix := &AppendEntriesArgs{Term: 1, LeaderId: "leader", PrevLogIndex: -1, Entries: entries[:1], LeaderCommit: 2}
	if _, err := node.AppendEntries(context.Background(), prefix); err != nil {
		t.Fatalf("prefix AppendEntries: %v", err)
	}
	if got := node.log.Len(); got != 3 {
		t.Fatalf("prefix RPC truncated the log to %d entries", got)
	}
}

// A conflicting entry below commitIndex must be rejected outright rather than
// silently discarding committed data.
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

	// Same index, different term, at an already-committed position.
	conflicting := []*LogEntry{{Cmd: "DEL", Term: 2, Index: 1, Data: []byte(`{"cmd":"DEL"}`)}}
	_, err := node.AppendEntries(context.Background(), &AppendEntriesArgs{
		Term: 2, LeaderId: "leader", PrevLogIndex: 0, PrevLogTerm: 1, Entries: conflicting, LeaderCommit: 1,
	})
	if err == nil {
		t.Fatal("expected an error when asked to overwrite a committed entry")
	}
}

// ReadIndex is the guard on linearizable reads: a node that is not the leader
// must refuse to serve one rather than answer from stale local state.
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

// A leader cut off from its followers must stop serving reads once its lease
// expires, even though it still believes it is the leader.
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

// A restarted node must replay its durable log into the state machine. It used
// to resume from a persisted lastApplied, skip every committed entry, and come
// back up with an empty store.
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
