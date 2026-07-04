package redis

import (
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/redis/data_structure"
)

func TestEvalAndResponseAllowsConcurrentReaders(t *testing.T) {
	store := NewRedisStoreWithEviction(nil, data_structure.EvictFirst).(*redisStore)

	store.mu.RLock()
	completed := make(chan struct{})

	go func() {
		_, _ = store.EvalAndResponse(&Command{Cmd: "PING"})
		close(completed)
	}()

	select {
	case <-completed:
	case <-time.After(100 * time.Millisecond):
		store.mu.RUnlock()
		t.Fatal("read-only command should be allowed while another reader holds the lock")
	}

	store.mu.RUnlock()
}
