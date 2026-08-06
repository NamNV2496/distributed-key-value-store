package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
)

const ConfChangeCommand = "__raft_conf__"

type Configuration struct {
	Voters   map[string]string `json:"voters"`
	Learners map[string]string `json:"learners,omitempty"`
}

func (c Configuration) Clone() Configuration {
	out := Configuration{Voters: make(map[string]string, len(c.Voters))}
	for id, addr := range c.Voters {
		out.Voters[id] = addr
	}
	if len(c.Learners) > 0 {
		out.Learners = make(map[string]string, len(c.Learners))
		for id, addr := range c.Learners {
			out.Learners[id] = addr
		}
	}
	return out
}

func (c Configuration) Members() map[string]string {
	out := make(map[string]string, len(c.Voters)+len(c.Learners))
	for id, addr := range c.Voters {
		out[id] = addr
	}
	for id, addr := range c.Learners {
		out[id] = addr
	}
	return out
}

var ErrConfChangeInFlight = errors.New("a configuration change is already in flight")

func (rn *RaftNode) Configuration() Configuration {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.configuration()
}

func (rn *RaftNode) configuration() Configuration {
	cfg := Configuration{
		Voters:   make(map[string]string, len(rn.voters)),
		Learners: make(map[string]string, len(rn.learners)),
	}
	for id, addr := range rn.voters {
		cfg.Voters[id] = addr
	}
	for id, addr := range rn.learners {
		cfg.Learners[id] = addr
	}
	return cfg
}

func (rn *RaftNode) IsVoter() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	_, ok := rn.voters[rn.nodeID]
	return ok
}

func (rn *RaftNode) ProposeConfChange(next Configuration) (int64, error) {
	if len(next.Voters) == 0 {
		return -1, errors.New("a configuration needs at least one voter")
	}

	data, err := json.Marshal(next)
	if err != nil {
		return -1, err
	}

	rn.mu.Lock()
	if rn.role != LeaderRole {
		rn.mu.Unlock()
		return -1, ErrNotLeader
	}
	if rn.pendingConfIndex > rn.commitIndex {
		rn.mu.Unlock()
		return -1, ErrConfChangeInFlight
	}
	if err := validateStep(rn.configuration(), next); err != nil {
		rn.mu.Unlock()
		return -1, err
	}

	term := rn.state.CurrentTerm()
	index := int64(rn.log.Len())
	if err := rn.log.Append(ConfChangeCommand, term, index, data); err != nil {
		rn.mu.Unlock()
		return -1, err
	}
	rn.pendingConfIndex = index
	rn.applyConfiguration(next)

	soleVoter := rn.quorum() <= 1
	if soleVoter {
		rn.commitIndex = index
		rn.state.SetCommitIndex(index)
		rn.commitCond.Broadcast()
	}
	hasClients := len(rn.clientMap) > 0
	rn.mu.Unlock()

	if hasClients {
		rn.sendAppendEntries()
	}
	return index, nil
}

func validateStep(cur, next Configuration) error {
	added, removed := 0, 0
	for id := range next.Voters {
		if _, ok := cur.Voters[id]; !ok {
			added++
		}
	}
	for id := range cur.Voters {
		if _, ok := next.Voters[id]; !ok {
			removed++
		}
	}
	if added+removed > 1 {
		return fmt.Errorf(
			"a configuration change may add or remove at most one voter at a time (this one adds %d and removes %d)",
			added, removed)
	}
	return nil
}

func (rn *RaftNode) applyConfiguration(cfg Configuration) {
	rn.voters = make(map[string]string, len(cfg.Voters))
	for id, addr := range cfg.Voters {
		rn.voters[id] = addr
	}
	rn.learners = make(map[string]string, len(cfg.Learners))
	for id, addr := range cfg.Learners {
		rn.learners[id] = addr
	}

	members := cfg.Members()
	rn.peers = members

	for id := range rn.clientMap {
		if _, still := members[id]; !still {
			delete(rn.clientMap, id)
			delete(rn.nextIndex, id)
			delete(rn.matchIndex, id)
			delete(rn.lastAck, id)
		}
	}
	for id, addr := range members {
		if id == rn.nodeID {
			continue
		}
		if _, exists := rn.clientMap[id]; exists {
			continue
		}
		rn.clientMap[id] = rn.newClient(addr)
		if rn.role == LeaderRole {
			rn.nextIndex[id] = int64(rn.log.Len())
			rn.matchIndex[id] = -1
		}
	}

	if _, stillMember := members[rn.nodeID]; !stillMember && rn.role == LeaderRole {
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}
	if _, votes := rn.voters[rn.nodeID]; !votes && rn.role == LeaderRole {
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}
}

func (rn *RaftNode) refreshConfigurationFromLog() {
	for i := int64(rn.log.Len()) - 1; i >= 0; i-- {
		entry := rn.log.GetEntry(i)
		if entry == nil || entry.Cmd != ConfChangeCommand {
			continue
		}
		var cfg Configuration
		if err := json.Unmarshal(entry.Data, &cfg); err != nil {
			log.Printf("[%s] ignoring unreadable configuration at index %d: %v", rn.nodeID, i, err)
			return
		}
		rn.applyConfiguration(cfg)
		return
	}
}

func (rn *RaftNode) maybePromoteLearner(peerID string) {
	if rn.role != LeaderRole {
		return
	}
	if _, isLearner := rn.learners[peerID]; !isLearner {
		return
	}
	if rn.pendingConfIndex > rn.commitIndex {
		return // one change at a time
	}
	if rn.matchIndex[peerID] < rn.commitIndex {
		return
	}

	next := rn.configuration()
	addr := next.Learners[peerID]
	delete(next.Learners, peerID)
	next.Voters[peerID] = addr

	go func() {
		if _, err := rn.ProposeConfChange(next); err != nil &&
			!errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrConfChangeInFlight) {
			log.Printf("[%s] could not promote %s to voter: %v", rn.nodeID, peerID, err)
		}
	}()
}
