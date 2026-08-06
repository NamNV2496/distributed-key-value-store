package redis

import "testing"

func TestShardOptionsUsesThreadsValue(t *testing.T) {
	t.Setenv("THREADS", "8")

	opts, err := shardOptionsFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Threads != 8 {
		t.Fatalf("expected 8 worker threads, got %d", opts.Threads)
	}
}

func TestShardOptionsDefaultsToPositiveThreadCount(t *testing.T) {
	t.Setenv("THREADS", "")

	opts, err := shardOptionsFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Threads <= 0 {
		t.Fatalf("expected a positive worker thread count, got %d", opts.Threads)
	}
}

func TestShardOptionsRejectsUnusablePort(t *testing.T) {
	t.Setenv("PORT", "not-a-port")

	if _, err := shardOptionsFromEnv(); err == nil {
		t.Fatal("expected an error for a non-numeric PORT")
	}
}

func TestRaftOptionsAlwaysIncludesItself(t *testing.T) {
	t.Setenv("NODE", "node2")
	t.Setenv("PEERS", "node1=http://node1:5000/")
	t.Setenv("ADVERTISE", "http://node2:5000/")

	opts := raftOptionsFromEnv()
	if got := opts.Peers["node2"]; got != "http://node2:5000" {
		t.Fatalf("expected the node's own trimmed address in Peers, got %q", got)
	}
	if got := opts.Peers["node1"]; got != "http://node1:5000" {
		t.Fatalf("expected the configured peer to survive, got %q", got)
	}
}
