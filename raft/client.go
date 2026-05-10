package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/wal"
)

// RaftRPCClient is the interface for Raft RPC communication
type RaftRPCClient interface {
	RequestVote(ctx context.Context, args *RequestVoteArgs, opts ...interface{}) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, args *AppendEntriesArgs, opts ...interface{}) (*AppendEntriesReply, error)
}

// RequestVoteArgs represents RequestVote RPC arguments
type RequestVoteArgs struct {
	Term         int64
	CandidateId  string
	LastLogIndex int64
	LastLogTerm  int64
}

// RequestVoteReply represents RequestVote RPC reply
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

// AppendEntriesArgs represents AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int64
	LeaderId     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*LogEntry
	LeaderCommit int64
}

// AppendEntriesReply represents AppendEntries RPC reply
type AppendEntriesReply struct {
	Term          int64
	NodeId        string
	Success       bool
	ConflictIndex int64
}

// LogEntry represents a single entry in the replicated log (alias for wal.LogEntry)
type LogEntry = wal.LogEntry

// HTTPRaftClient implements HTTP-based Raft RPC communication
type HTTPRaftClient struct {
	mu      sync.Mutex
	baseURL string
	client  *http.Client
}

// NewRaftRPCClient creates a new HTTP-based Raft client
func NewRaftRPCClient(baseURL string) RaftRPCClient {
	return &HTTPRaftClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// RequestVote sends a RequestVote RPC to a peer via HTTP
func (c *HTTPRaftClient) RequestVote(ctx context.Context, args *RequestVoteArgs, _ ...interface{}) (*RequestVoteReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/raft/vote", c.baseURL), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request vote failed: %s", string(body))
	}

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

// AppendEntries sends an AppendEntries RPC to a peer via HTTP
func (c *HTTPRaftClient) AppendEntries(ctx context.Context, args *AppendEntriesArgs, _ ...interface{}) (*AppendEntriesReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/raft/append", c.baseURL), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("append entries failed: %s", string(body))
	}

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}

	return &reply, nil
}
