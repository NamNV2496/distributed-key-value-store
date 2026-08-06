package shard

import (
	"errors"
	"log"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/raft"
)

const membershipStepInterval = 300 * time.Millisecond

func shardPeerURLs(shard *cluster.Shard) map[string]string {
	out := make(map[string]string, len(shard.Members))
	for nodeID, addr := range shard.Members {
		out[nodeID] = addr + "/shards/" + shard.ID
	}
	return out
}

func (m *Manager) membershipLoop() {
	ticker := time.NewTicker(membershipStepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.stepMembership()
		}
	}
}

func (m *Manager) stepMembership() {
	topo := m.topo.Get()
	if topo == nil {
		return
	}

	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return
	}
	groups := make(map[string]*raft.RaftNode, len(m.groups))
	for id, server := range m.groups {
		groups[id] = server.RaftNode()
	}
	m.mu.RUnlock()

	for id, node := range groups {
		shard, ok := topo.Shards[id]
		if !ok {
			m.retireGroup(id)
			continue
		}
		if _, mine := shard.Members[m.nodeID]; !mine {
			if _, stillInGroup := node.Configuration().Members()[m.nodeID]; !stillInGroup {
				m.retireGroup(id)
				continue
			}
			leader := node.GetLeaderID()
			if node.GetRole() != raft.LeaderRole && leader != "" && leader != m.nodeID {
				m.retireGroup(id)
				continue
			}
		}
		if node.GetRole() != raft.LeaderRole {
			continue
		}
		next, step, ok := nextConfiguration(node.Configuration(), shardPeerURLs(shard), m.nodeID)
		if !ok {
			continue
		}
		if _, err := node.ProposeConfChange(next); err != nil {
			if !errors.Is(err, raft.ErrConfChangeInFlight) && !errors.Is(err, raft.ErrNotLeader) {
				log.Printf("[%s/%s] membership change failed: %v", m.nodeID, id, err)
			}
			continue
		}
		log.Printf("[%s/%s] %s", m.nodeID, id, step)
	}
}

func (m *Manager) retireGroup(shardID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	server, running := m.groups[shardID]
	if !running || m.closed {
		return
	}
	log.Printf("[%s] shard %s left the group; stopping its Raft node", m.nodeID, shardID)
	server.Stop()
	delete(m.groups, shardID)
}

func nextConfiguration(
	cur raft.Configuration, desired map[string]string, self string,
) (raft.Configuration, string, bool) {
	next := cur.Clone()
	present := cur.Members()

	for id, addr := range desired {
		if _, in := present[id]; in {
			continue
		}
		if next.Learners == nil {
			next.Learners = map[string]string{}
		}
		next.Learners[id] = addr
		return next, "adding " + id + " as a learner", true
	}

	for id := range desired {
		if _, votes := next.Voters[id]; !votes {
			return raft.Configuration{}, "", false
		}
	}

	for id := range next.Learners {
		if _, want := desired[id]; !want {
			delete(next.Learners, id)
			return next, "dropping learner " + id, true
		}
	}

	var leaving string
	for id := range next.Voters {
		if _, want := desired[id]; want {
			continue
		}
		if leaving == "" || id != self {
			leaving = id
		}
	}
	if leaving != "" && len(next.Voters) > 1 {
		delete(next.Voters, leaving)
		return next, "removing voter " + leaving, true
	}

	return raft.Configuration{}, "", false
}
