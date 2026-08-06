package raft

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type persistedState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex int64  `json:"commit_index"`
}

type State struct {
	mu          sync.Mutex
	currentTerm int64
	votedFor    string
	commitIndex int64
	nodeID      string
	stateFile   string
	closed      bool
}

func (s *State) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func NewState(nodeID, stateFile string) *State {
	s := &State{
		nodeID:      nodeID,
		stateFile:   stateFile,
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
	s.commitIndex = ps.CommitIndex
	return s
}

func (s *State) save() {
	if s.stateFile == "" || s.closed {
		return
	}
	ps := persistedState{
		CurrentTerm: s.currentTerm,
		VotedFor:    s.votedFor,
		CommitIndex: s.commitIndex,
	}
	data, err := json.Marshal(ps)
	if err != nil {
		log.Printf("[%s] failed to marshal raft state: %v", s.nodeID, err)
		return
	}
	if err := atomicWriteFile(s.stateFile, data); err != nil {
		// A node that cannot persist its term cannot participate safely.
		log.Printf("[%s] FATAL: failed to persist raft state: %v", s.nodeID, err)
	}
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

func (s *State) SetTermAndVotedFor(term int64, candidateID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = candidateID
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
