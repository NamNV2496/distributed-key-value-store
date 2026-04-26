package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/wal"
)

// NodeRole represents the role of a node in the cluster
type NodeRole string
type NodeState int

const (
	FollowerRole  NodeRole  = "Follower"
	LeaderRole    NodeRole  = "Leader"
	CandidateRole NodeRole  = "Candidate"
	Follower      NodeState = iota
	Candidate
	Leader
)

// RaftNode represents a Raft consensus node
type RaftNode struct {
	mu sync.RWMutex

	// RaftNode represents a Raft consensus node
	// Note: peers map now contains HTTP URLs instead of gRPC addresses
	nodeID    string
	peers     map[string]string // nodeID -> HTTP URL (e.g., "http://localhost:5001")
	clientMap map[string]RaftRPCClient

	// Raft state
	state            *State
	role             NodeRole
	log              *wal.WAL
	commitIndex      int64
	lastApplied      int64
	leaderId         string
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// Leader state
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Channel for apply
	applyChan chan wal.LogEntry
	stopChan  chan struct{}
	stopped   bool

	// Callbacks
	onStateChange    func(role NodeRole)
	onLogEntriesRead func(entries []wal.LogEntry)
}

// Config holds configuration for RaftNode
type Config struct {
	NodeID           string
	Peers            map[string]string // nodeID -> gRPC address
	LogFile          string
	StateFile        string
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	OnStateChange    func(role NodeRole)
	OnLogEntriesRead func(entries []wal.LogEntry)
}

// NewRaftNode creates a new Raft node
func NewRaftNode(cfg Config) (*RaftNode, error) {
	logWAL, err := wal.NewWAL(cfg.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = time.Millisecond * time.Duration(150+rand.Intn(150))
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 50 * time.Millisecond
	}

	node := &RaftNode{
		nodeID:           cfg.NodeID,
		peers:            cfg.Peers,
		clientMap:        make(map[string]RaftRPCClient),
		state:            NewState(cfg.NodeID, cfg.StateFile),
		role:             FollowerRole,
		log:              logWAL,
		commitIndex:      -1,
		lastApplied:      -1,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		nextIndex:        make(map[string]int64),
		matchIndex:       make(map[string]int64),
		applyChan:        make(chan wal.LogEntry, 100),
		stopChan:         make(chan struct{}),
		onStateChange:    cfg.OnStateChange,
		onLogEntriesRead: cfg.OnLogEntriesRead,
	}

	// Initialize HTTP clients for peers
	for peerID, addr := range cfg.Peers {
		if peerID == cfg.NodeID {
			continue
		}
		node.clientMap[peerID] = NewRaftRPCClient(addr)
	}

	return node, nil
}

// Start starts the Raft node
func (rn *RaftNode) Start() {
	go rn.run()
	go rn.applyLogEntries()
}

// Stop stops the Raft node
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	rn.stopped = true
	rn.mu.Unlock()
	close(rn.stopChan)
	rn.log.Close()
}

func (rn *RaftNode) run() {
	rn.mu.Lock()
	hbInterval := rn.heartbeatTimeout
	rn.mu.Unlock()
	heartbeatTicker := time.NewTicker(hbInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			rn.mu.Lock()
			isLeader := rn.role == LeaderRole
			rn.mu.Unlock()
			if isLeader {
				go rn.sendHeartbeats()
			}
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			role := rn.role
			leaderId := rn.leaderId
			rn.mu.Unlock()
			// If there's already a known leader, don't start an election
			if leaderId != "" {
				rn.mu.Lock()
				rn.resetElectionTimer()
				rn.mu.Unlock()
			} else if role != LeaderRole {
				go rn.campaign(false)
			} else {
				rn.mu.Lock()
				rn.resetElectionTimer()
				rn.mu.Unlock()
			}
		case <-rn.stopChan:
			return
		}
	}
}

// AppendEntry appends a new entry to the log
func (rn *RaftNode) AppendEntry(data []byte) error {
	rn.mu.RLock()
	if rn.role != LeaderRole {
		rn.mu.RUnlock()
		return fmt.Errorf("not leader")
	}
	term := rn.state.CurrentTerm()
	rn.mu.RUnlock()

	index := int64(rn.log.Len())
	return rn.log.Append(term, index, data)
}

// GetLeaderID returns the current leader ID
func (rn *RaftNode) GetLeaderID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderId
}

// GetRole returns the current role
func (rn *RaftNode) GetRole() NodeRole {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.role
}

// GetCurrentTerm returns the current term
func (rn *RaftNode) GetCurrentTerm() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state.CurrentTerm()
}

// ApplyChan returns the apply channel for log entries
func (rn *RaftNode) ApplyChan() <-chan LogEntry {
	return rn.applyChan
}

// RequestVote handles RequestVote RPC
func (rn *RaftNode) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	currentTerm := rn.state.CurrentTerm()
	reply := &RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: false,
	}

	// If candidate's term is less than current term, deny vote
	if args.Term < currentTerm {
		return reply, nil
	}

	// If candidate's term is greater, update term and reset voted for
	if args.Term > currentTerm {
		rn.state.SetCurrentTerm(args.Term)
		currentTerm = args.Term
		reply.Term = currentTerm
	}

	// Check if already voted for someone else
	if rn.state.HasVoted() && rn.state.VotedFor() != args.CandidateId {
		return reply, nil
	}

	// Check if candidate's log is at least as up-to-date as mine
	lastEntry := rn.log.LastEntry()
	var lastTerm, lastIndex int64
	if lastEntry != nil {
		lastTerm = lastEntry.Term
		lastIndex = lastEntry.Index
	} else {
		lastIndex = -1
	}

	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		return reply, nil
	}

	// Grant vote
	rn.state.SetVotedFor(args.CandidateId)
	reply.VoteGranted = true

	// Reset election timer
	rn.resetElectionTimer()

	return reply, nil
}

// AppendEntries handles AppendEntries RPC
func (rn *RaftNode) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	currentTerm := rn.state.CurrentTerm()
	reply := &AppendEntriesReply{
		Term:    currentTerm,
		Success: false,
	}

	// If leader's term is less than current term, deny
	if args.Term < currentTerm {
		return reply, nil
	}

	// If leader's term is greater, update term
	if args.Term > currentTerm {
		rn.state.SetCurrentTerm(args.Term)
		currentTerm = args.Term
		reply.Term = currentTerm
	}

	// Update leader ID and reset election timer
	rn.leaderId = args.LeaderId
	rn.resetElectionTimer()

	// Check if log contains entry at prevLogIndex with prevLogTerm
	if args.PrevLogIndex >= 0 {
		prevEntry := rn.log.GetEntry(args.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
			reply.ConflictIndex = int64(rn.log.Len())
			return reply, nil
		}
	}

	// Append entries
	if len(args.Entries) > 0 {
		// Truncate log if there are conflicting entries
		if args.PrevLogIndex+1 < int64(rn.log.Len()) {
			rn.log.TruncateAfter(args.PrevLogIndex)
		}

		// Append new entries
		for i, entry := range args.Entries {
			idx := args.PrevLogIndex + 1 + int64(i)
			rn.log.Append(entry.Term, idx, entry.Data)
		}
	}

	reply.Success = true

	// Update commitIndex
	if args.LeaderCommit > rn.commitIndex {
		lastIndex := int64(rn.log.Len() - 1)
		if args.LeaderCommit > lastIndex {
			rn.commitIndex = lastIndex
		} else {
			rn.commitIndex = args.LeaderCommit
		}
	}

	return reply, nil
}

// runElectionTimer runs the election timer
func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-rn.stopChan:
			return
		default:
		}

		rn.mu.Lock()
		if rn.electionTimer != nil {
			rn.electionTimer.Stop()
		}
		timeout := rn.electionTimeout
		rn.mu.Unlock()

		rn.mu.Lock()
		rn.electionTimer = time.AfterFunc(timeout, func() {
			rn.startElection()
		})
		rn.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// resetElectionTimer resets the election timer
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	rn.electionTimer = time.AfterFunc(rn.electionTimeout, func() {
		rn.startElection()
	})
}

// startElection starts a leader election
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	if rn.stopped {
		rn.mu.Unlock()
		return
	}

	newTerm := rn.state.CurrentTerm() + 1
	rn.state.SetCurrentTerm(newTerm)
	rn.role = CandidateRole
	rn.leaderId = ""
	rn.state.SetVotedFor(rn.nodeID)

	lastEntry := rn.log.LastEntry()
	var lastLogIndex, lastLogTerm int64
	if lastEntry != nil {
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	} else {
		lastLogIndex = -1
		lastLogTerm = 0
	}

	if rn.onStateChange != nil {
		rn.onStateChange(rn.role)
	}

	rn.mu.Unlock()

	// Request votes from peers
	rn.requestVotes(newTerm, lastLogIndex, lastLogTerm)
}

// requestVotes requests votes from all peers
func (rn *RaftNode) requestVotes(term, lastLogIndex, lastLogTerm int64) {
	var wg sync.WaitGroup
	votes := 1 // Vote for self
	votesMu := sync.Mutex{}

	for peerID, client := range rn.clientMap {
		wg.Add(1)
		go func(id string, cli RaftRPCClient) {
			defer wg.Done()

			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rn.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			reply, err := cli.RequestVote(ctx, args)
			if err != nil {
				return
			}

			rn.mu.Lock()
			if reply.Term > rn.state.CurrentTerm() {
				rn.state.SetCurrentTerm(reply.Term)
				rn.role = FollowerRole
				rn.leaderId = ""
				if rn.onStateChange != nil {
					rn.onStateChange(rn.role)
				}
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(peerID, client)
	}

	wg.Wait()

	// Check if we have majority
	// Cluster size = peers + 1 (self)
	clusterSize := len(rn.peers) + 1
	majorityNeeded := clusterSize/2 + 1
	votesMu.Lock()
	hasVotes := votes >= majorityNeeded
	votesMu.Unlock()

	if hasVotes {
		rn.mu.Lock()
		if rn.role == CandidateRole && rn.state.CurrentTerm() == term {
			rn.role = LeaderRole
			rn.leaderId = rn.nodeID

			// Initialize leader state
			for peerID := range rn.peers {
				if peerID != rn.nodeID {
					rn.nextIndex[peerID] = int64(rn.log.Len())
					rn.matchIndex[peerID] = -1
				}
			}

			if rn.onStateChange != nil {
				rn.onStateChange(rn.role)
			}

			rn.mu.Unlock()

			// Start sending heartbeats
			go rn.sendHeartbeats()
		} else {
			rn.mu.Unlock()
		}
	}
}

// sendHeartbeats sends heartbeats to all followers
func (rn *RaftNode) sendHeartbeats() {
	ticker := time.NewTicker(rn.heartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopChan:
			return
		case <-ticker.C:
			rn.mu.RLock()
			if rn.role != LeaderRole {
				rn.mu.RUnlock()
				return
			}
			currentTerm := rn.state.CurrentTerm()
			rn.mu.RUnlock()

			rn.sendAppendEntries(currentTerm, true)
		}
	}
}

// sendAppendEntries sends AppendEntries RPC to all followers
func (rn *RaftNode) sendAppendEntries(term int64, isHeartbeat bool) {
	for peerID, client := range rn.clientMap {
		go rn.sendAppendEntriesTo(peerID, client, term, isHeartbeat)
	}
}

// sendAppendEntriesTo sends AppendEntries RPC to a specific peer
func (rn *RaftNode) sendAppendEntriesTo(peerID string, client RaftRPCClient, term int64, isHeartbeat bool) {
	rn.mu.RLock()

	nextIdx := rn.nextIndex[peerID]
	if nextIdx < 0 {
		nextIdx = 0
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm int64

	if prevLogIndex >= 0 {
		prevEntry := rn.log.GetEntry(prevLogIndex)
		if prevEntry != nil {
			prevLogTerm = prevEntry.Term
		}
	}

	entries := make([]*LogEntry, 0)
	if !isHeartbeat {
		logEntries := rn.log.GetEntries(nextIdx)
		for _, entry := range logEntries {
			entries = append(entries, &LogEntry{
				Term:  entry.Term,
				Index: entry.Index,
				Data:  entry.Data,
			})
		}
	}

	currentTerm := rn.state.CurrentTerm()
	rn.mu.RUnlock()

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rn.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	reply, err := client.AppendEntries(ctx, args)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > currentTerm {
		rn.state.SetCurrentTerm(reply.Term)
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
		return
	}

	if !reply.Success {
		if reply.ConflictIndex > 0 {
			rn.nextIndex[peerID] = reply.ConflictIndex - 1
			if rn.nextIndex[peerID] < 0 {
				rn.nextIndex[peerID] = 0
			}
		}
		return
	}

	// Success: update nextIndex and matchIndex
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		rn.nextIndex[peerID] = lastIndex + 1
		rn.matchIndex[peerID] = lastIndex
	}
}

// applyLogEntries applies committed log entries to the state machine
func (rn *RaftNode) applyLogEntries() {
	for {
		select {
		case <-rn.stopChan:
			return
		default:
		}

		rn.mu.RLock()
		if rn.commitIndex > rn.lastApplied {
			for i := rn.lastApplied + 1; i <= rn.commitIndex; i++ {
				entry := rn.log.GetEntry(i)
				if entry != nil {
					rn.applyChan <- wal.LogEntry{
						Term:  entry.Term,
						Index: entry.Index,
						Data:  entry.Data,
					}
					rn.lastApplied = i
				}
			}
		}
		rn.mu.RUnlock()

		time.Sleep(10 * time.Millisecond)
	}
}
