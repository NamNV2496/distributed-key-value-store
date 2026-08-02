package redis

import "testing"

func TestRateLimitInitAndCheck(t *testing.T) {
	store := NewRedisStore(nil).(*redisStore)

	initRes, err := store.EvalAndResponse(&Command{Cmd: "RATELIMIT_INIT", Args: map[string]string{"key": "foo", "limit": "2", "window_ms": "1000", "type": "fixed"}})
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if initRes.(map[string]any)["initialized"] != true {
		t.Fatalf("init response should mark limiter initialized")
	}

	checkRes, err := store.EvalAndResponse(&Command{Cmd: "RATELIMIT_CHECK", Args: map[string]string{"key": "foo", "limit": "2", "window_ms": "1000", "type": "fixed"}})
	if err != nil {
		t.Fatalf("check failed: %v", err)
	}
	result, ok := checkRes.(map[string]any)
	if !ok {
		t.Fatalf("check should return a map, got %T", checkRes)
	}
	if result["allowed"] != true {
		t.Fatalf("first check should be allowed")
	}
}
