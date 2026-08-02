package raft

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// persistedState is the on-disk layout of the durable node state.
type persistedState struct {
	CurrentTerm int64  `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex int64  `json:"commit_index"`
}

// State holds Raft's durable fields and syncs them to disk on every change.
//
// lastApplied is deliberately NOT persisted. The state machine lives in memory
// and there are no snapshots, so a restarted node starts empty; remembering how
// far it had applied would make it skip replaying the log and silently lose
// every key. See the lastApplied comment in NewRaftNode.
type State struct {
	mu          sync.Mutex
	currentTerm int64
	votedFor    string
	commitIndex int64
	nodeID      string
	stateFile   string
	closed      bool
}

// Close stops further persistence. Called when the node shuts down so a
// straggling goroutine cannot recreate the state file after the process (or a
// test's temp directory) has torn it down.
func (s *State) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

// NewState loads persisted state from stateFile (if it exists) and returns a
// ready-to-use State. Missing file is treated as a clean first start.
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

// save writes state to disk; caller must hold s.mu.
//
// Raft's safety argument assumes currentTerm and votedFor survive a crash, so
// the write must be both atomic and durable. os.WriteFile is neither: it
// truncates in place (a crash mid-write leaves a torn file, and a node that
// reads back a lower term can vote twice in the same term) and it never
// fsyncs. Instead we write a temp file, fsync it, rename it over the target —
// rename is atomic within a directory — and fsync the directory so the rename
// itself is durable.
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

// atomicWriteFile writes data to path via a fsynced temp file and an atomic
// rename, then fsyncs the containing directory.
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

	// fsync the directory so the rename survives a crash.
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

// SetTermAndVotedFor persists both fields in a single atomic write.
//
// Calling SetCurrentTerm then SetVotedFor costs two fsynced writes, and every
// vote — granted or cast — pays it. Since a durable write is ~10ms, that is
// enough to blow an election timeout and keep a cluster campaigning forever.
// Both fields change together on every path that matters, so write them once.
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
