package raft

import (
	"context"

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
	Success       bool
	ConflictIndex int64
}

// LogEntry represents a single entry in the replicated log (alias for wal.LogEntry)
type LogEntry = wal.LogEntry
