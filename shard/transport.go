package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

func (m *Manager) shardCommand(ctx context.Context, shardID string, cmd *redis.Command) (any, string, error) {
	if g, ok := m.group(shardID); ok {
		result, err := g.SubmitCommand(ctx, cmd)
		return result, m.nodeID, err
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return nil, "", err
	}
	return m.peer.PostToShardMember(ctx, shardID, body, "", routing.ShardCommandURL)
}

func (m *Manager) broadcastTopology(ctx context.Context, topo *cluster.Topology, extra ...*cluster.Topology) []string {
	body, err := json.Marshal(topo)
	if err != nil {
		return []string{fmt.Sprintf("marshal: %v", err)}
	}

	targets := cluster.NodeAddresses(append([]*cluster.Topology{topo}, extra...)...)
	ids := make([]string, 0, len(targets))
	for id := range targets {
		if id != m.nodeID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	var failures []string
	for _, id := range ids {
		if err := m.pushTopology(ctx, targets[id], body); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", id, err))
			log.Printf("[%s] topology v%d not accepted by %s: %v", m.nodeID, topo.Version, id, err)
		}
	}
	return failures
}

func (m *Manager) pushTopology(ctx context.Context, baseURL string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/cluster/topology", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, routing.MaxBodyBytes))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", resp.Status)
	}
	return nil
}

func (m *Manager) remoteShardLeader(ctx context.Context, shardID string) (string, error) {
	topo := m.topo.Get()
	shard, ok := topo.Shards[shardID]
	if !ok {
		return "", fmt.Errorf("unknown shard %q", shardID)
	}
	var errs []error
	for _, nodeID := range shard.NodeIDs() {
		if nodeID == m.nodeID {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			shard.Members[nodeID]+"/shards/"+shardID+"/status", nil)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		resp, err := m.httpClient.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var status struct {
			LeaderID string `json:"leader_id"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, routing.MaxBodyBytes)).Decode(&status)
		resp.Body.Close()
		if decodeErr != nil {
			errs = append(errs, decodeErr)
			continue
		}
		if status.LeaderID != "" {
			return status.LeaderID, nil
		}
	}
	return "", errors.Join(errs...)
}
