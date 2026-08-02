package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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
const (
	NoOpCommand         = "__raft_noop__"
	maxEntriesPerAppend = 256
)

type RaftNode struct {
	mu         sync.RWMutex
	commitCond *sync.Cond
	nodeID     string
	peers      map[string]string // nodeID -> HTTP URL (e.g., "http://localhost:5001")
	clientMap  map[string]RaftRPCClient

	// Raft state
	state            *State
	role             NodeRole
	log              *wal.WAL
	commitIndex      int64
	lastApplied      int64 // highest index handed to the state machine
	appliedIndex     int64 // highest index the state machine confirmed
	leaderId         string
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// Leader state
	nextIndex       map[string]int64
	matchIndex      map[string]int64
	lastAck         map[string]time.Time
	campaigning     bool
	lastHeard       time.Time
	electionResetCh chan struct{}

	// Channel for apply
	applyChan chan wal.LogEntry
	stopChan  chan struct{}
	stopOnce  sync.Once
	stopped   bool

	// Callbacks
	newClient        func(addr string) RaftRPCClient
	onStateChange    func(role NodeRole)
	onLogEntriesRead func(entries []wal.LogEntry)
}

// ErrNotLeader is returned by operations that only a leader may serve.
var ErrNotLeader = errors.New("not leader")

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
	NewClient        func(addr string) RaftRPCClient
}

// NewRaftNode creates a new Raft node
func NewRaftNode(cfg Config) (*RaftNode, error) {
	logWAL, err := wal.NewWAL(cfg.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 500 * time.Millisecond
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 100 * time.Millisecond
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
		lastApplied:      -1,
		appliedIndex:     -1,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		nextIndex:        make(map[string]int64),
		matchIndex:       make(map[string]int64),
		lastAck:          make(map[string]time.Time),
		applyChan:        make(chan wal.LogEntry, 100),
		stopChan:         make(chan struct{}),
		electionResetCh:  make(chan struct{}, 1),
		onStateChange:    cfg.OnStateChange,
		onLogEntriesRead: cfg.OnLogEntriesRead,
	}
	node.commitCond = sync.NewCond(&node.mu)

	newClient := cfg.NewClient
	if newClient == nil {
		newClient = NewRaftRPCClient
	}
	for peerID, addr := range cfg.Peers {
		if peerID == cfg.NodeID {
			continue
		}
		node.clientMap[peerID] = newClient(addr)
	}
	node.newClient = newClient

	return node, nil
}

// Start starts the Raft node
func (rn *RaftNode) Start() {
	rn.mu.RLock()
	singleNode := len(rn.clientMap) == 0
	rn.mu.RUnlock()

	go rn.run()
	go rn.applyLogEntries()

	// Single-node cluster: skip the election timeout and become leader now.
	if singleNode {
		go rn.campaignFindLeaderNode(false)
	}
}

// randomElectionTimeout draws a fresh timeout from [base, 2*base).
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	base := rn.electionTimeout
	return base + time.Duration(rand.Int63n(int64(base)))
}

// Stop stops the Raft node. Safe to call more than once.
func (rn *RaftNode) Stop() {
	rn.stopOnce.Do(func() {
		rn.mu.Lock()
		rn.stopped = true
		rn.mu.Unlock()
		close(rn.stopChan)
		rn.commitCond.Broadcast() // wake any blocked WaitCommit / applyLogEntries
		rn.state.Close()
		if err := rn.log.Close(); err != nil {
			log.Printf("[%s] error closing WAL: %v", rn.nodeID, err)
		}
	})
}

func (rn *RaftNode) run() {
	timer := time.NewTimer(rn.randomElectionTimeout())
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(rn.randomElectionTimeout())
	}

	for {
		select {
		case <-rn.stopChan:
			return

		case <-rn.electionResetCh:
			resetTimer()

		case <-timer.C:
			rn.mu.Lock()
			role := rn.role
			if role != LeaderRole {
				// Clear stale leader hint so the next election isn't blocked.
				rn.leaderId = ""
			}
			rn.mu.Unlock()
			resetTimer()
			if role != LeaderRole {
				go rn.campaignFindLeaderNode(true)
			}
		}
	}
}

func (rn *RaftNode) Propose(cmd string, data []byte) (int64, error) {
	rn.mu.Lock()
	if rn.role != LeaderRole {
		rn.mu.Unlock()
		return -1, ErrNotLeader
	}
	term := rn.state.CurrentTerm()
	index := int64(rn.log.Len())
	if err := rn.log.Append(cmd, term, index, data); err != nil {
		rn.mu.Unlock()
		return -1, err
	}
	singleNode := len(rn.clientMap) == 0
	if singleNode {
		rn.commitIndex = index
		rn.state.SetCommitIndex(index)
		rn.commitCond.Broadcast()
	}
	rn.mu.Unlock()

	if singleNode {
		return index, nil
	}
	rn.sendAppendEntries(false)
	return index, nil
}

func (rn *RaftNode) AddPeer(nodeID, addr string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, exists := rn.clientMap[nodeID]; exists {
		return // already known
	}
	rn.peers[nodeID] = addr
	rn.clientMap[nodeID] = rn.newClient(addr)
	if rn.role == LeaderRole {
		rn.nextIndex[nodeID] = 0
		rn.matchIndex[nodeID] = -1
	}
	log.Printf("[%s] added peer %s at %s", rn.nodeID, nodeID, addr)
}

func (rn *RaftNode) ReplicateTo(nodeID string) {
	rn.mu.RLock()
	client, ok := rn.clientMap[nodeID]
	rn.mu.RUnlock()
	if !ok {
		return
	}
	go rn.sendAppendEntriesTo(nodeID, client, false)
}

func (rn *RaftNode) GetPeers() map[string]string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	out := make(map[string]string, len(rn.clientMap))
	for id := range rn.clientMap {
		out[id] = rn.peers[id]
	}
	return out
}

func (rn *RaftNode) WaitCommit(ctx context.Context, index int64) error {
	stop := context.AfterFunc(ctx, func() { rn.commitCond.Broadcast() })
	defer stop()

	rn.mu.Lock()
	defer rn.mu.Unlock()
	for rn.commitIndex < index {
		select {
		case <-rn.stopChan:
			return errors.New("node stopped")
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rn.commitCond.Wait()
	}
	return nil
}

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
	if args.Term < currentTerm {
		return reply, nil
	}
	logOK := func() bool {
		lastEntry := rn.log.LastEntry()
		var lastTerm, lastIndex int64
		if lastEntry != nil {
			lastTerm = lastEntry.Term
			lastIndex = lastEntry.Index
		} else {
			lastIndex = -1
		}
		if args.LastLogTerm != lastTerm {
			return args.LastLogTerm > lastTerm
		}
		return args.LastLogIndex >= lastIndex
	}
	if args.PreVote {
		if !rn.lastHeard.IsZero() && time.Since(rn.lastHeard) < rn.electionTimeout {
			return reply, nil
		}
		reply.VoteGranted = logOK()
		return reply, nil
	}

	// If candidate's term is greater, step down and clear stale leader hint.
	newTerm := args.Term > currentTerm
	if newTerm {
		currentTerm = args.Term
		reply.Term = currentTerm
		rn.role = FollowerRole
		rn.leaderId = ""
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}

	// Check if already voted for someone else in this term.
	alreadyVoted := !newTerm && rn.state.HasVoted() && rn.state.VotedFor() != args.CandidateId
	if alreadyVoted || !logOK() {
		if newTerm {
			rn.state.SetCurrentTerm(args.Term) // adopt the term even when denying
		}
		return reply, nil
	}

	// Grant the vote, persisting term and vote in one durable write.
	rn.state.SetTermAndVotedFor(currentTerm, args.CandidateId)
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

	// If leader's term is greater, adopt it.
	if args.Term > currentTerm {
		rn.state.SetCurrentTerm(args.Term)
		currentTerm = args.Term
		reply.Term = currentTerm
	}
	if rn.role != FollowerRole {
		rn.role = FollowerRole
		if rn.onStateChange != nil {
			rn.onStateChange(rn.role)
		}
	}
	rn.leaderId = args.LeaderId
	rn.lastHeard = time.Now()
	rn.resetElectionTimer()
	if args.PrevLogIndex >= 0 {
		prevEntry := rn.log.GetEntry(args.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
			reply.ConflictIndex = int64(rn.log.Len())
			return reply, nil
		}
	}
	var toAppend []wal.LogEntry
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + int64(i)

		if len(toAppend) == 0 && idx < int64(rn.log.Len()) {
			existing := rn.log.GetEntry(idx)
			if existing != nil && existing.Term == entry.Term {
				continue
			}
			if idx <= rn.commitIndex {
				return nil, fmt.Errorf(
					"refusing to truncate committed entry at index %d (commitIndex=%d)", idx, rn.commitIndex)
			}
			if err := rn.log.TruncateAfter(idx - 1); err != nil {
				return nil, err
			}
		}

		toAppend = append(toAppend, wal.LogEntry{
			Cmd:   entry.Cmd,
			Term:  entry.Term,
			Index: idx,
			Data:  entry.Data,
		})
	}
	if err := rn.log.AppendBatch(toAppend); err != nil {
		return nil, err
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
		rn.state.SetCommitIndex(rn.commitIndex)
		rn.commitCond.Broadcast()
	}

	return reply, nil
}

func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.electionResetCh <- struct{}{}:
	default:
	}
}

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
		logEntries := rn.log.GetEntries(nextIdx, maxEntriesPerAppend)
		for _, entry := range logEntries {
			entries = append(entries, &LogEntry{
				Cmd:   entry.Cmd,
				Term:  entry.Term,
				Index: entry.Index,
				Data:  entry.Data,
			})
		}
	}

	if rn.role != LeaderRole {
		rn.mu.RUnlock()
		return
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
	if rn.role != LeaderRole || rn.state.CurrentTerm() != currentTerm {
		return
	}

	if !reply.Success {
		if reply.ConflictIndex >= 0 {
			rn.nextIndex[peerID] = reply.ConflictIndex
			if rn.nextIndex[peerID] < 0 {
				rn.nextIndex[peerID] = 0
			}
		}
		return
	}

	rn.lastAck[peerID] = time.Now()

	matched := prevLogIndex + int64(len(entries))
	if matched > rn.matchIndex[peerID] {
		rn.matchIndex[peerID] = matched
	}
	if matched+1 > rn.nextIndex[peerID] {
		rn.nextIndex[peerID] = matched + 1
	}
	rn.advanceCommitIndex()
}

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

		var toApply []wal.LogEntry
		for rn.commitIndex > rn.lastApplied {
			rn.lastApplied++
			entry := rn.log.GetEntry(rn.lastApplied)
			if entry != nil {
				toApply = append(toApply, *entry)
			}
		}
		rn.mu.Unlock()

		for _, e := range toApply {
			select {
			case rn.applyChan <- e:
			case <-rn.stopChan:
				rn.mu.Lock()
				return
			}
		}

		rn.mu.Lock()
	}
}

func (rn *RaftNode) MarkApplied(index int64) {
	rn.mu.Lock()
	if index > rn.appliedIndex {
		rn.appliedIndex = index
		rn.commitCond.Broadcast()
	}
	rn.mu.Unlock()
}

func (rn *RaftNode) AppliedIndex() int64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.appliedIndex
}

func (rn *RaftNode) WaitApplied(ctx context.Context, index int64) error {
	stop := context.AfterFunc(ctx, func() { rn.commitCond.Broadcast() })
	defer stop()

	rn.mu.Lock()
	defer rn.mu.Unlock()
	for rn.appliedIndex < index {
		select {
		case <-rn.stopChan:
			return errors.New("node stopped")
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rn.commitCond.Wait()
	}
	return nil
}

func (rn *RaftNode) ReadIndex(ctx context.Context) (int64, error) {
	rn.mu.RLock()
	if rn.role != LeaderRole {
		rn.mu.RUnlock()
		return -1, ErrNotLeader
	}
	term := rn.state.CurrentTerm()
	idx := rn.commitIndex
	rn.mu.RUnlock()

	if err := rn.confirmLeadership(ctx, term); err != nil {
		return -1, err
	}
	return idx, nil
}
func (rn *RaftNode) hasQuorumLease() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.role != LeaderRole {
		return false
	}
	fresh := 1 // ourselves
	for _, at := range rn.lastAck {
		if time.Since(at) < rn.electionTimeout {
			fresh++
		}
	}
	return fresh >= rn.quorum()
}

func (rn *RaftNode) confirmLeadership(ctx context.Context, term int64) error {
	rn.mu.RLock()
	clients := make(map[string]RaftRPCClient, len(rn.clientMap))
	for peerID, client := range rn.clientMap {
		clients[peerID] = client
	}
	commit := rn.commitIndex
	needed := rn.quorum()
	rn.mu.RUnlock()

	if 1 >= needed { // single-node cluster: we are the quorum
		return nil
	}

	if rn.hasQuorumLease() {
		return nil
	}

	type ack struct {
		ok   bool
		term int64
	}
	ackCh := make(chan ack, len(clients))
	for _, client := range clients {
		go func(client RaftRPCClient) {
			cctx, cancel := context.WithTimeout(ctx, rn.electionTimeout)
			defer cancel()
			reply, err := client.AppendEntries(cctx, &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rn.nodeID,
				PrevLogIndex: -1,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: commit,
			})
			if err != nil {
				ackCh <- ack{}
				return
			}
			ackCh <- ack{ok: reply.Success, term: reply.Term}
		}(client)
	}

	votes := 1 // ourselves
	for range clients {
		select {
		case a := <-ackCh:
			if a.term > term {
				return ErrNotLeader // somebody has moved on without us
			}
			if a.ok {
				votes++
				if votes >= needed {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("lost quorum: cannot confirm leadership")
}
