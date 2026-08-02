package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/wal"
)

// NodeRole represents the role of a node in the cluster
type NodeRole string
type NodeState int

const (
	FollowerRole  NodeRole = "Follower"
	LeaderRole    NodeRole = "Leader"
	CandidateRole NodeRole = "Candidate"
)

// RaftNode represents a Raft consensus node
type RaftNode struct {
	mu         sync.RWMutex
	commitCond *sync.Cond // broadcast when commitIndex advances

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

	// HTTP client used only for health-check pings (not Raft RPCs).
	healthClient *http.Client

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

	st := NewState(cfg.NodeID, cfg.StateFile)
	node := &RaftNode{
		nodeID:           cfg.NodeID,
		peers:            cfg.Peers,
		clientMap:        make(map[string]RaftRPCClient),
		state:            st,
		role:             FollowerRole,
		log:              logWAL,
		commitIndex:      st.CommitIndex(),
		lastApplied:      st.LastApplied(),
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		nextIndex:        make(map[string]int64),
		matchIndex:       make(map[string]int64),
		applyChan:        make(chan wal.LogEntry, 100),
		stopChan:         make(chan struct{}),
		healthClient:     &http.Client{Timeout: 300 * time.Millisecond},
		onStateChange:    cfg.OnStateChange,
		onLogEntriesRead: cfg.OnLogEntriesRead,
	}
	node.commitCond = sync.NewCond(&node.mu)

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
	rn.mu.Lock()
	rn.electionTimer = time.NewTimer(rn.electionTimeout)
	singleNode := len(rn.clientMap) == 0
	rn.mu.Unlock()
	go rn.run()
	go rn.applyLogEntries()
	go rn.runHealthChecker()

	// Single-node cluster: skip the election timeout and become leader now.
	if singleNode {
		go rn.campaignFindLeaderNode(false)
	}
}

// runHealthChecker periodically pings every peer and, when the current leader
// becomes unreachable for 2 consecutive checks, immediately triggers a new
// election rather than waiting for the election timer to fire.
//
// Once a node is confirmed dead it is skipped on every normal tick; only a
// recovery probe (every 2 s) is sent so we detect when it comes back.
func (rn *RaftNode) runHealthChecker() {
	const (
		failThreshold    = 2
		recoveryInterval = 2 * time.Second
		tickInterval     = 200 * time.Millisecond
	)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	fails := make(map[string]int)
	dead := make(map[string]bool)
	lastRecoveryPing := make(map[string]time.Time)

	for {
		select {
		case <-rn.stopChan:
			return
		case <-ticker.C:
			rn.mu.RLock()
			role := rn.role
			leaderId := rn.leaderId
			peers := make(map[string]string, len(rn.clientMap))
			for id := range rn.clientMap {
				peers[id] = rn.peers[id]
			}
			rn.mu.RUnlock()

			for nodeID, url := range peers {
				if dead[nodeID] {
					// Skip until the recovery window opens.
					if time.Since(lastRecoveryPing[nodeID]) < recoveryInterval {
						continue
					}
					lastRecoveryPing[nodeID] = time.Now()
					if rn.pingNode(url) {
						log.Printf("[health][%s] node %s recovered", rn.nodeID, nodeID)
						dead[nodeID] = false
						fails[nodeID] = 0
					}
					continue
				}

				// Normal liveness check.
				if rn.pingNode(url) {
					fails[nodeID] = 0
					continue
				}

				fails[nodeID]++
				log.Printf("[health][%s] node %s unreachable (attempt %d/%d)",
					rn.nodeID, nodeID, fails[nodeID], failThreshold)

				if fails[nodeID] >= failThreshold {
					dead[nodeID] = true
					lastRecoveryPing[nodeID] = time.Now()
					log.Printf("[health][%s] node %s marked DEAD", rn.nodeID, nodeID)

					if nodeID == leaderId && role != LeaderRole {
						log.Printf("[health][%s] leader %s is dead — triggering re-election", rn.nodeID, nodeID)
						rn.mu.Lock()
						if rn.leaderId == nodeID {
							rn.leaderId = ""
						}
						rn.mu.Unlock()
						// Random jitter (0-50 ms) to reduce split-vote probability.
						delay := time.Duration(rand.Intn(50)) * time.Millisecond
						time.AfterFunc(delay, func() { go rn.campaignFindLeaderNode(false) })
					}
				}
			}

			// If every known peer is unreachable this node is effectively alone —
			// promote to leader immediately without waiting for an election timeout.
			if role != LeaderRole && len(peers) > 0 {
				allDead := true
				for id := range peers {
					if !dead[id] {
						allDead = false
						break
					}
				}
				if allDead {
					log.Printf("[health][%s] all peers unreachable — promoting to leader", rn.nodeID)
					rn.mu.Lock()
					if rn.role != LeaderRole {
						rn.state.SetCurrentTerm(rn.state.CurrentTerm() + 1)
						rn.state.SetVotedFor(rn.nodeID)
						rn.role = LeaderRole
						rn.leaderId = rn.nodeID
						if rn.onStateChange != nil {
							rn.onStateChange(rn.role)
						}
						rn.mu.Unlock()
						go rn.sendHeartbeats()
					} else {
						rn.mu.Unlock()
					}
				}
			}
		}
	}
}

// pingNode sends a GET /health request and returns true if the node is alive.
func (rn *RaftNode) pingNode(url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := rn.healthClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Stop stops the Raft node
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	rn.stopped = true
	rn.mu.Unlock()
	close(rn.stopChan)
	rn.commitCond.Broadcast() // wake any blocked WaitCommit / applyLogEntries
	rn.log.Close()
}

func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			role := rn.role
			if role != LeaderRole {
				// Clear stale leader hint so the next election isn't blocked.
				rn.leaderId = ""
			}
			rn.resetElectionTimer()
			rn.mu.Unlock()
			if role != LeaderRole {
				go rn.campaignFindLeaderNode(false)
			}
		case <-rn.stopChan:
			return
		}
	}
}

// Propose appends a command to the leader's log, replicates it immediately,
// and returns the log index so the caller can wait for it to be committed.
func (rn *RaftNode) Propose(cmd string, data []byte) (int64, error) {
	rn.mu.Lock()
	if rn.role != LeaderRole {
		rn.mu.Unlock()
		return -1, fmt.Errorf("not leader")
	}
	term := rn.state.CurrentTerm()
	index := int64(rn.log.Len())
	if err := rn.log.Append(cmd, term, index, data); err != nil {
		rn.mu.Unlock()
		return -1, err
	}

	if len(rn.clientMap) == 0 {
		rn.commitIndex = index
		rn.state.SetCommitIndex(index)
		rn.commitCond.Broadcast()
	}
	rn.mu.Unlock()

	if len(rn.clientMap) == 0 {
		return index, nil
	}

	// Replicate immediately rather than waiting for the next heartbeat.
	rn.sendAppendEntries(false)
	return index, nil
}

// AddPeer adds a new node to the cluster at runtime. Safe to call concurrently.
func (rn *RaftNode) AddPeer(nodeID, addr string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, exists := rn.clientMap[nodeID]; exists {
		return // already known
	}
	rn.peers[nodeID] = addr
	rn.clientMap[nodeID] = NewRaftRPCClient(addr)
	if rn.role == LeaderRole {
		// Start from the beginning so the new node receives the full log.
		rn.nextIndex[nodeID] = 0
		rn.matchIndex[nodeID] = -1
	}
	log.Printf("[%s] added peer %s at %s", rn.nodeID, nodeID, addr)
}

// ReplicateTo immediately sends the full log to a specific peer.
// Called right after a new node joins so it doesn't have to wait for the
// next heartbeat to start receiving entries.
func (rn *RaftNode) ReplicateTo(nodeID string) {
	rn.mu.RLock()
	client, ok := rn.clientMap[nodeID]
	rn.mu.RUnlock()
	if !ok {
		return
	}
	go rn.sendAppendEntriesTo(nodeID, client, false)
}

// GetPeers returns a snapshot of current peer addresses (excludes self).
func (rn *RaftNode) GetPeers() map[string]string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	out := make(map[string]string, len(rn.clientMap))
	for id := range rn.clientMap {
		out[id] = rn.peers[id]
	}
	return out
}

// WaitCommit blocks until commitIndex >= index or the context is done.
func (rn *RaftNode) WaitCommit(ctx context.Context, index int64) error {
	// Wake the cond when ctx is cancelled so Wait() exits without polling.
	stop := context.AfterFunc(ctx, func() { rn.commitCond.Broadcast() })
	defer stop()

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if len(rn.clientMap) == 0 && rn.role == LeaderRole && index <= rn.commitIndex {
		return nil
	}
	for rn.commitIndex < index {
		select {
		case <-rn.stopChan:
			return fmt.Errorf("node stopped")
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rn.commitCond.Wait()
	}
	return nil
}

// advanceCommitIndex advances commitIndex to the highest index replicated
// to a quorum of nodes. Caller must hold rn.mu (write lock).
func (rn *RaftNode) advanceCommitIndex() {
	n := int64(rn.log.Len() - 1)
	for n > rn.commitIndex {
		count := 1 // leader itself
		for _, mi := range rn.matchIndex {
			if mi >= n {
				count++
			}
		}
		if count >= rn.quorum() {
			entry := rn.log.GetEntry(n)
			if entry != nil && entry.Term == rn.state.CurrentTerm() {
				rn.commitIndex = n
				rn.state.SetCommitIndex(n)
				rn.commitCond.Broadcast()
				return
			}
		}
		n--
	}
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

// GetCommitIndex returns the current commit index.
func (rn *RaftNode) GetCommitIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIndex
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

	// If candidate's term is greater, step down and clear stale leader hint.
	if args.Term > currentTerm {
		rn.state.SetCurrentTerm(args.Term)
		currentTerm = args.Term
		reply.Term = currentTerm
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
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
		// Truncate log if there are conflicting entries.
		if args.PrevLogIndex+1 < int64(rn.log.Len()) {
			if err := rn.log.TruncateAfter(args.PrevLogIndex); err != nil {
				return nil, err
			}
		}

		// Append new entries
		for i, entry := range args.Entries {
			idx := args.PrevLogIndex + 1 + int64(i)
			if err := rn.log.Append(entry.Cmd, entry.Term, idx, entry.Data); err != nil {
				return nil, err
			}
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
		rn.commitCond.Broadcast()
	}

	return reply, nil
}

// resetElectionTimer resets the election timer. Caller must hold rn.mu.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
		// Drain any pending tick so run() doesn't see a stale fire.
		select {
		case <-rn.electionTimer.C:
		default:
		}
	}
	rn.electionTimer = time.NewTimer(rn.electionTimeout)
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
			rn.mu.RUnlock()

			rn.sendAppendEntries(true)
		}
	}
}

// sendAppendEntries sends AppendEntries RPC to all followers
func (rn *RaftNode) sendAppendEntries(isHeartbeat bool) {
	rn.mu.RLock()
	clients := make(map[string]RaftRPCClient, len(rn.clientMap))
	for peerID, client := range rn.clientMap {
		clients[peerID] = client
	}
	rn.mu.RUnlock()

	for peerID, client := range clients {
		go rn.sendAppendEntriesTo(peerID, client, isHeartbeat)
	}
}

// sendAppendEntriesTo sends AppendEntries RPC to a specific peer
func (rn *RaftNode) sendAppendEntriesTo(peerID string, client RaftRPCClient, isHeartbeat bool) {
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
				Cmd:   entry.Cmd,
				Term:  entry.Term,
				Index: entry.Index,
				Data:  entry.Data,
			})
		}
	}

	currentTerm := rn.state.CurrentTerm()
	leaderCommit := rn.commitIndex
	rn.mu.RUnlock()

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rn.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
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
		// Use ConflictIndex as the new nextIndex so the leader retries from
		// exactly where the follower diverges.  The >= 0 check also handles
		// ConflictIndex==0 (completely empty new node), which the old "> 0"
		// guard skipped — leaving nextIndex stuck and replication stalled.
		if reply.ConflictIndex >= 0 {
			rn.nextIndex[peerID] = reply.ConflictIndex
			if rn.nextIndex[peerID] < 0 {
				rn.nextIndex[peerID] = 0
			}
		}
		return
	}

	// Success: update nextIndex and matchIndex, then try to advance commitIndex.
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		rn.nextIndex[peerID] = lastIndex + 1
		rn.matchIndex[peerID] = lastIndex
		rn.advanceCommitIndex()
	}
}

// applyLogEntries applies committed log entries to the state machine.
func (rn *RaftNode) applyLogEntries() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	for {
		select {
		case <-rn.stopChan:
			return
		default:
		}

		if rn.commitIndex <= rn.lastApplied {
			rn.commitCond.Wait()
			continue
		}

		// Collect entries while holding the lock, then send without it.
		var toApply []wal.LogEntry
		for rn.commitIndex > rn.lastApplied {
			rn.lastApplied++
			entry := rn.log.GetEntry(rn.lastApplied)
			if entry != nil {
				toApply = append(toApply, wal.LogEntry{
					Cmd:   entry.Cmd,
					Term:  entry.Term,
					Index: entry.Index,
					Data:  entry.Data,
				})
			}
		}
		if len(toApply) > 0 {
			rn.state.SetLastApplied(rn.lastApplied)
		}
		rn.mu.Unlock()

		for _, e := range toApply {
			rn.applyChan <- e
		}

		rn.mu.Lock()
	}
}
