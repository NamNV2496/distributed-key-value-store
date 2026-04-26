package raft

import (
	"sync"
)

// State represents the node's persistent state in Raft
type State struct {
	mu          sync.RWMutex
	currentTerm int64
	votedFor    string
	nodeID      string
	stateFile   string
}

// NewState creates a new state instance
func NewState(nodeID, stateFile string) *State {
	return &State{
		nodeID:    nodeID,
		stateFile: stateFile,
	}
}

// CurrentTerm returns the current term
func (s *State) CurrentTerm() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

// SetCurrentTerm sets the current term
func (s *State) SetCurrentTerm(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = ""
}

// VotedFor returns the candidate ID this node voted for in the current term
func (s *State) VotedFor() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

// SetVotedFor sets the candidate ID this node voted for
func (s *State) SetVotedFor(candidateID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateID
}

// HasVoted checks if this node has already voted in the current term
func (s *State) HasVoted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor != ""
}

// NodeID returns the node ID
func (s *State) NodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}
