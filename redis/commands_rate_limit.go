package redis

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

type rateLimiterState struct {
	kind       string
	limit      int64
	windowMs   int64
	startTime  int64
	count      int64
	lastRefill time.Time
	available  float64
	mu         sync.Mutex
}

func (s *redisStore) cmdRateLimitInit(args map[string]string) any {
	key := args["key"]
	if key == "" {
		return errors.New("ERR missing key")
	}
	limit, err := strconv.ParseInt(args["limit"], 10, 64)
	if err != nil || limit <= 0 {
		return errors.New("ERR invalid limit")
	}
	windowMs, err := strconv.ParseInt(args["window_ms"], 10, 64)
	if err != nil || windowMs <= 0 {
		return errors.New("ERR invalid window_ms")
	}
	typeName := strings.ToLower(args["type"])
	if typeName == "" {
		typeName = "fixed"
	}

	if _, ok := s.rateLimiters[key]; !ok {
		s.initRateLimiter(key, typeName, limit, windowMs)
	}
	return map[string]any{"initialized": true, "key": key, "type": typeName}
}

func (s *redisStore) cmdRateLimitCheck(args map[string]string) any {
	key := args["key"]
	if key == "" {
		return errors.New("ERR missing key")
	}
	limit, err := strconv.ParseInt(args["limit"], 10, 64)
	if err != nil || limit <= 0 {
		return errors.New("ERR invalid limit")
	}
	windowMs, err := strconv.ParseInt(args["window_ms"], 10, 64)
	if err != nil || windowMs <= 0 {
		return errors.New("ERR invalid window_ms")
	}
	typeName := strings.ToLower(args["type"])
	if typeName == "" {
		typeName = "fixed"
	}

	state, ok := s.rateLimiters[key]
	if !ok {
		state = s.initRateLimiter(key, typeName, limit, windowMs)
	} else {
		state.kind = typeName
		state.limit = limit
		state.windowMs = windowMs
	}

	allowed, remaining, retryAfterMs := state.check(typeName, limit, windowMs)
	result := map[string]any{"allowed": allowed, "remaining": remaining, "retry_after_ms": retryAfterMs, "type": typeName}
	return result
}

func (s *redisStore) initRateLimiter(key, kind string, limit, windowMs int64) *rateLimiterState {
	state := &rateLimiterState{
		kind:       kind,
		limit:      limit,
		windowMs:   windowMs,
		startTime:  time.Now().UnixMilli(),
		lastRefill: time.Now(),
		available:  float64(limit),
	}
	s.rateLimiters[key] = state
	return state
}

func (s *rateLimiterState) check(kind string, limit, windowMs int64) (bool, int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	if kind == "token" {
		elapsed := now.Sub(s.lastRefill).Seconds()
		if elapsed > 0 {
			refill := elapsed * float64(limit) / float64(windowMs/1000)
			s.available += refill
			if s.available > float64(limit) {
				s.available = float64(limit)
			}
			s.lastRefill = now
		}
		if s.available >= 1 {
			s.available -= 1
			return true, int64(s.available), 0
		}
		return false, int64(s.available), int64((1 - s.available) * 1000)
	}

	if kind == "sliding" {
		if s.count > 0 && now.UnixMilli()-s.startTime > windowMs {
			s.count = 0
			s.startTime = now.UnixMilli()
		}
		if s.count < limit {
			s.count++
			return true, limit - s.count, 0
		}
		return false, 0, windowMs - (now.UnixMilli() - s.startTime)
	}

	if s.count >= limit {
		if now.UnixMilli()-s.startTime < windowMs {
			return false, 0, windowMs - (now.UnixMilli() - s.startTime)
		}
		s.count = 0
		s.startTime = now.UnixMilli()
	}
	if s.count < limit {
		s.count++
		return true, limit - s.count, 0
	}
	return false, 0, 0
}
