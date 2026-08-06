package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/namnv2496/go-redis-raft/shard/raft/wal"
)

// RaftRPCClient is the interface for Raft RPC communication
type RaftRPCClient interface {
	RequestVote(ctx context.Context, args *RequestVoteArgs, opts ...interface{}) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, args *AppendEntriesArgs, opts ...interface{}) (*AppendEntriesReply, error)
}

type RequestVoteArgs struct {
	Term         int64
	CandidateId  string
	LastLogIndex int64
	LastLogTerm  int64
	PreVote      bool
}

type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term          int64
	NodeId        string
	Success       bool
	ConflictIndex int64
}

type LogEntry = wal.LogEntry

type HTTPRaftClient struct {
	baseURL string
	client  *http.Client
}

func NewRaftRPCClient(baseURL string) RaftRPCClient {
	return &HTTPRaftClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *HTTPRaftClient) RequestVote(ctx context.Context, args *RequestVoteArgs, _ ...interface{}) (*RequestVoteReply, error) {
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

func (c *HTTPRaftClient) AppendEntries(ctx context.Context, args *AppendEntriesArgs, _ ...interface{}) (*AppendEntriesReply, error) {
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
