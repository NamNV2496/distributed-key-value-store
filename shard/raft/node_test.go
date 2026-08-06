package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSingleNodeProposeCommitsImmediately(t *testing.T) {
	dir := t.TempDir()
	node, err := NewRaftNode(Config{
		NodeID:    "node1",
		Peers:     map[string]string{},
		LogFile:   filepath.Join(dir, "node1.log"),
		StateFile: filepath.Join(dir, "node1.state"),
	})
	if err != nil {
		t.Fatalf("new raft node: %v", err)
	}

	node.Start()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.GetRole() == LeaderRole {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if node.GetRole() != LeaderRole {
		t.Fatalf("expected node to become leader, got %s", node.GetRole())
	}

	index, _, err := node.Propose("SET", []byte(`{"cmd":"SET"}`))
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := node.WaitCommit(ctx, index); err != nil {
		t.Fatalf("wait commit failed: %v", err)
	}
	if got := node.GetCommitIndex(); got < index {
		t.Fatalf("expected commit index %d, got %d", index, got)
	}
}
