package redis

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/shard/raft"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

// newSingleNodeServer boots a one-node cluster with a live apply loop, backed
// by files under dir so the same files can be reopened to test restart.
func newSingleNodeServer(t *testing.T, dir string) (*RaftRedisServer, IRedisStore, *raft.RaftNode) {
	t.Helper()

	node, err := raft.NewRaftNode(raft.Config{
		NodeID:    "solo",
		Peers:     map[string]string{},
		LogFile:   filepath.Join(dir, "solo.wal"),
		StateFile: filepath.Join(dir, "solo.state"),
	})
	if err != nil {
		t.Fatalf("new raft node: %v", err)
	}
	store := NewRedisStoreWithEviction(node, data_structure.EvictFirst)
	server, err := NewRaftRedisServer("solo", node, store, map[string]string{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go store.RunApplyLoop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && node.GetRole() != raft.LeaderRole {
		time.Sleep(5 * time.Millisecond)
	}
	if node.GetRole() != raft.LeaderRole {
		t.Fatal("node never became leader")
	}
	return server, store, node
}

func exec(t *testing.T, s *RaftRedisServer, cmd string, args map[string]string) any {
	t.Helper()
	body, err := json.Marshal(Command{Cmd: cmd, Args: args})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := s.executeCommand(ctx, "", body)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return res
}

// A write must return what the command actually produced. Every write used to
// return a hardcoded "OK", throwing away INCR's new value, DEL's count, and so
// on.
func TestWriteReturnsRealCommandResult(t *testing.T) {
	server, _, node := newSingleNodeServer(t, t.TempDir())
	defer node.Stop()

	for want := int64(1); want <= 3; want++ {
		got := exec(t, server, "INCR", map[string]string{"key": "counter"})
		if got != want {
			t.Fatalf("INCR returned %v (%T), want %d — real result discarded", got, got, want)
		}
	}

	if got := exec(t, server, "SET", map[string]string{"key": "k", "value": "v"}); got != "OK" {
		t.Fatalf("SET returned %v, want OK", got)
	}
	if got := exec(t, server, "DEL", map[string]string{"key": "k"}); got != int64(1) {
		t.Fatalf("DEL returned %v, want a delete count of 1", got)
	}
}

// A client must be able to read back a value it has just written. The write
// path used to return once the entry was committed, while the apply loop ran
// asynchronously behind it, so an immediate read could miss the write.
func TestReadYourOwnWrite(t *testing.T) {
	server, _, node := newSingleNodeServer(t, t.TempDir())
	defer node.Stop()

	for i := 0; i < 200; i++ {
		exec(t, server, "SET", map[string]string{"key": "race", "value": "v"})
		got := exec(t, server, "GET", map[string]string{"key": "race"})
		if got != "v" {
			t.Fatalf("iteration %d: wrote v, read back %v — response returned before apply", i, got)
		}
		exec(t, server, "DEL", map[string]string{"key": "race"})
	}
}

// Restarting must rebuild the in-memory store from the durable log.
func TestRestartRebuildsStoreFromLog(t *testing.T) {
	dir := t.TempDir()

	server, _, node := newSingleNodeServer(t, dir)
	exec(t, server, "SET", map[string]string{"key": "persisted", "value": "yes"})
	exec(t, server, "INCR", map[string]string{"key": "n"})
	exec(t, server, "INCR", map[string]string{"key": "n"})
	if got := exec(t, server, "GET", map[string]string{"key": "persisted"}); got != "yes" {
		t.Fatalf("pre-restart read: %v", got)
	}
	node.Stop()

	// Reopen the same files, as a process restart would.
	server2, _, node2 := newSingleNodeServer(t, dir)
	defer node2.Stop()

	if got := exec(t, server2, "GET", map[string]string{"key": "persisted"}); got != "yes" {
		t.Fatalf("after restart GET persisted = %v, want \"yes\" — the log was not replayed", got)
	}
	if got := exec(t, server2, "GET", map[string]string{"key": "n"}); got != "2" {
		t.Fatalf("after restart GET n = %v, want \"2\"", got)
	}
}

// The no-op a new leader appends carries no payload; the apply loop must skip
// it and still acknowledge it, otherwise appliedIndex stalls and every
// subsequent write blocks forever.
func TestNoOpEntryDoesNotStallApply(t *testing.T) {
	server, _, node := newSingleNodeServer(t, t.TempDir())
	defer node.Stop()

	// becomeLeader already proposed a no-op; a write after it must complete.
	if got := exec(t, server, "SET", map[string]string{"key": "after-noop", "value": "1"}); got != "OK" {
		t.Fatalf("write after the leader no-op returned %v", got)
	}
	if node.AppliedIndex() < node.GetCommitIndex() {
		t.Fatalf("appliedIndex %d lags commitIndex %d", node.AppliedIndex(), node.GetCommitIndex())
	}
}

// Every command that mutates state must be routed through Raft, or replicas
// silently diverge.
func TestAllMutatingCommandsAreReplicated(t *testing.T) {
	mutating := []string{
		"SET", "DEL", "EXPIRE", "INCR",
		"SADD", "SREM", "SPOP",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMAX", "ZPOPMIN",
		"GEOADD",
		"BF_RESERVE", "BF_MADD",
		"CMS_INITBYDIM", "CMS_INITBYPROB", "CMS_INCRBY",
		"SL_ADD", "SL_DELETE",
	}
	for _, cmd := range mutating {
		if !isWriteCommand(cmd) {
			t.Errorf("%s mutates the store but is not a write command — it would apply on one node only", cmd)
		}
		if isReadOnlyCommand(cmd) {
			t.Errorf("%s is classified both as a write and as read-only", cmd)
		}
	}
}
