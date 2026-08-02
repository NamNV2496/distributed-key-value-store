package redis

import "testing"

func TestPatternSubscriptionMatchesWildcard(t *testing.T) {
	store := NewRedisStore(nil).(*redisStore)

	_, err := store.EvalAndResponse(&Command{Cmd: "PSUBSCRIBE", Args: map[string]string{"channel": "abc*"}})
	if err != nil {
		t.Fatalf("psubscribe failed: %v", err)
	}

	res, err := store.EvalAndResponse(&Command{Cmd: "PUBLISH", Args: map[string]string{"channel": "abc123", "message": "hello"}})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if res != int64(1) {
		t.Fatalf("expected 1 subscriber to receive the message, got %v", res)
	}
}
