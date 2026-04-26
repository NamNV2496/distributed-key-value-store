package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/raft"
)

type IRedisClient interface {
	Set(ctx context.Context, key string, val any, expire time.Duration) error
	Get(ctx context.Context, key string) (any, error)
	Delete(ctx context.Context, key string) error
	IsLeader() bool
	GetLeaderID() string
}

type RedisDataDeps struct {
	RaftNode *raft.RaftNode
}

type RedisData struct {
	value     any
	expiredAt time.Time
	createdAt time.Time
	updatedAt time.Time
}

type RedisClient struct {
	mu       sync.RWMutex
	data     map[string]RedisData
	raftNode *raft.RaftNode
}

func NewRedisClient(deps RedisDataDeps) IRedisClient {
	client := &RedisClient{
		data:     make(map[string]RedisData),
		raftNode: deps.RaftNode,
	}

	// Start applying log entries from Raft
	if deps.RaftNode != nil {
		go client.applyLogEntries()
		go client.startExpireWorker()
	}

	return client
}

// applyLogEntries applies committed log entries to the Redis state machine
func (rc *RedisClient) applyLogEntries() {
	if rc.raftNode == nil {
		return
	}

	for entry := range rc.raftNode.ApplyChan() {
		var cmd map[string]interface{}
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			continue
		}

		op, ok := cmd["op"].(string)
		if !ok {
			continue
		}

		switch op {
		case "set":
			key, _ := cmd["key"].(string)
			val, _ := cmd["value"].(string)
			expireMs, _ := cmd["expire_ms"].(float64)
			rc.applySet(key, val, time.Duration(int64(expireMs))*time.Millisecond)

		case "delete":
			key, _ := cmd["key"].(string)
			rc.applyDelete(key)
		}
	}
}

// applySet applies a set command to the state machine
func (rc *RedisClient) applySet(key string, val string, expire time.Duration) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	now := time.Now()
	existing, exists := rc.data[key]

	rc.data[key] = RedisData{
		value:     val,
		expiredAt: now.Add(expire),
		createdAt: existing.createdAt,
		updatedAt: now,
	}

	if !exists {
		rc.data[key] = RedisData{
			value:     val,
			expiredAt: now.Add(expire),
			createdAt: now,
			updatedAt: now,
		}
	}
}

// applyDelete applies a delete command to the state machine
func (rc *RedisClient) applyDelete(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	delete(rc.data, key)
}

// startExpireWorker removes expired keys
func (rc *RedisClient) startExpireWorker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		rc.mu.Lock()
		now := time.Now()
		for key, data := range rc.data {
			if now.After(data.expiredAt) {
				delete(rc.data, key)
			}
		}
		rc.mu.Unlock()
	}
}

// Set adds/updates a key-value pair (must go through leader)
func (rc *RedisClient) Set(ctx context.Context, key string, val any, expire time.Duration) error {
	if rc.raftNode == nil {
		return fmt.Errorf("raft node not configured")
	}

	// Only leader can accept writes
	if rc.raftNode.GetRole() != raft.LeaderRole {
		return fmt.Errorf("not leader, leader is %s", rc.raftNode.GetLeaderID())
	}

	// Create command
	cmd := map[string]interface{}{
		"op":        "set",
		"key":       key,
		"value":     fmt.Sprintf("%v", val),
		"expire_ms": expire.Milliseconds(),
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Append to Raft log
	if err := rc.raftNode.AppendEntry(data); err != nil {
		return fmt.Errorf("failed to append entry: %w", err)
	}

	return nil
}

// Get retrieves a value by key (can be served from any node)
func (rc *RedisClient) Get(ctx context.Context, key string) (any, error) {
	rc.mu.RLock()
	data, exist := rc.data[key]
	rc.mu.RUnlock()

	if !exist {
		return nil, fmt.Errorf("key not found")
	}

	// Check if expired
	if time.Now().After(data.expiredAt) {
		return nil, fmt.Errorf("key expired")
	}

	return data.value, nil
}

// Delete removes a key (must go through leader)
func (rc *RedisClient) Delete(ctx context.Context, key string) error {
	if rc.raftNode == nil {
		return fmt.Errorf("raft node not configured")
	}

	// Only leader can accept writes
	if rc.raftNode.GetRole() != raft.LeaderRole {
		return fmt.Errorf("not leader, leader is %s", rc.raftNode.GetLeaderID())
	}

	// Create command
	cmd := map[string]interface{}{
		"op":  "delete",
		"key": key,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Append to Raft log
	if err := rc.raftNode.AppendEntry(data); err != nil {
		return fmt.Errorf("failed to append entry: %w", err)
	}

	return nil
}

// IsLeader returns whether this node is the leader
func (rc *RedisClient) IsLeader() bool {
	if rc.raftNode == nil {
		return false
	}
	return rc.raftNode.GetRole() == raft.LeaderRole
}

// GetLeaderID returns the current leader ID
func (rc *RedisClient) GetLeaderID() string {
	if rc.raftNode == nil {
		return ""
	}
	return rc.raftNode.GetLeaderID()
}
