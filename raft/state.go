package raft

import (
	"encoding/json"
	"os"
	"sync"
)

// persistedState is the on-disk layout of the durable node state.
type persistedState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	LastApplied int64  `json:"last_applied"`
	CommitIndex int64  `json:"commit_index"`
}

// State holds Raft's durable fields and syncs them to disk on every change.
// All four fields survive a restart so the node does not re-apply old log
// entries or reset its election term.
type State struct {
	mu          sync.Mutex
	currentTerm int64
	votedFor    string
	lastApplied int64
	commitIndex int64
	nodeID      string
	stateFile   string
}

// NewState loads persisted state from stateFile (if it exists) and returns a
// ready-to-use State. Missing file is treated as a clean first start.
func NewState(nodeID, stateFile string) *State {
	s := &State{
		nodeID:      nodeID,
		stateFile:   stateFile,
		lastApplied: -1,
		commitIndex: -1,
	}
	if stateFile == "" {
		return s
	}
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return s // file absent on first start
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return s // corrupt file — start fresh
	}
	s.currentTerm = ps.CurrentTerm
	s.votedFor = ps.VotedFor
	s.lastApplied = ps.LastApplied
	s.commitIndex = ps.CommitIndex
	return s
}

// save writes state to disk; caller must hold s.mu.
func (s *State) save() {
	if s.stateFile == "" {
		return
	}
	ps := persistedState{
		CurrentTerm: s.currentTerm,
		VotedFor:    s.votedFor,
		LastApplied: s.lastApplied,
		CommitIndex: s.commitIndex,
	}
	data, _ := json.Marshal(ps)
	_ = os.WriteFile(s.stateFile, data, 0600)
}

func (s *State) CurrentTerm() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

func (s *State) SetCurrentTerm(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = ""
	s.save()
}

func (s *State) VotedFor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor
}

func (s *State) SetVotedFor(candidateID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateID
	s.save()
}

func (s *State) HasVoted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor != ""
}

func (s *State) NodeID() string {
	return s.nodeID
}

func (s *State) LastApplied() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

func (s *State) SetLastApplied(idx int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = idx
	s.save()
}

func (s *State) CommitIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitIndex
}

func (s *State) SetCommitIndex(idx int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = idx
	s.save()
}
