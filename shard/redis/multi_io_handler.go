package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/shard/raft"
)

// maxCommandBodyBytes bounds a single client command payload.
const maxCommandBodyBytes = 8 << 20 // 8 MiB

type commandRequest struct {
	cmd       string
	body      []byte
	ctx       context.Context
	response  chan commandResponse
	requestID string
}

type commandResponse struct {
	result any
	err    error
}

type commandHTTPResponse struct {
	NodeID  string `json:"node_id"`
	Command string `json:"command,omitempty"`
	Status  string `json:"status"`
	Result  any    `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

type RaftRedisServer struct {
	NodeID     string
	ShardID    string
	raftNode   *raft.RaftNode
	redisStore IRedisStore

	// peers maps a node ID to this shard's Raft group on that node, e.g.
	// "http://node2:5000/shards/shard-a". Stored complete, in the same form as
	// raft.Config.Peers, because a node hosts several groups on one port: a bare
	// member address would not say which group a forwarded command belongs to.
	peers map[string]string

	kvMu          sync.RWMutex
	commandQueue  chan commandRequest
	stopExecution chan struct{}
	executionDone chan struct{}
	executionOnce sync.Once
}

func NewRaftRedisServer(
	nodeID string,
	raftNode *raft.RaftNode,
	redisStore IRedisStore,
	peers map[string]string,
) (*RaftRedisServer, error) {
	return NewShardRedisServer("", nodeID, raftNode, redisStore, peers)
}

func NewShardRedisServer(
	shardID string,
	nodeID string,
	raftNode *raft.RaftNode,
	redisStore IRedisStore,
	peers map[string]string,
) (*RaftRedisServer, error) {
	raftNode.Start()
	return &RaftRedisServer{
		NodeID:        nodeID,
		ShardID:       shardID,
		raftNode:      raftNode,
		redisStore:    redisStore,
		peers:         peers,
		kvMu:          sync.RWMutex{},
		commandQueue:  make(chan commandRequest, 256),
		stopExecution: make(chan struct{}),
		executionDone: make(chan struct{}),
	}, nil
}

func (s *RaftRedisServer) RaftNode() *raft.RaftNode { return s.raftNode }

func (s *RaftRedisServer) Store() IRedisStore { return s.redisStore }

func (s *RaftRedisServer) Submit(ctx context.Context, body []byte) (any, error) {
	leaderID := s.raftNode.GetLeaderID()
	if leaderID == "" {
		return nil, fmt.Errorf("shard %s: leader unknown", s.shardLabel())
	}

	if leaderID != s.NodeID {
		resp, err := s.forwardToLeader(ctx, body)
		if err != nil {
			return nil, fmt.Errorf("shard %s: forward to leader %s: %w", s.shardLabel(), leaderID, err)
		}
		defer resp.Body.Close()

		var decoded commandHTTPResponse
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxCommandBodyBytes)).Decode(&decoded); err != nil {
			return nil, fmt.Errorf("shard %s: decode leader response: %w", s.shardLabel(), err)
		}
		if decoded.Error != "" {
			return nil, errors.New(decoded.Error)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("shard %s: leader returned %s", s.shardLabel(), resp.Status)
		}
		return decoded.Result, nil
	}

	respCh := make(chan commandResponse, 1)
	select {
	case s.commandQueue <- commandRequest{body: body, ctx: ctx, response: respCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case resp := <-respCh:
		return resp.result, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RaftRedisServer) SubmitCommand(ctx context.Context, cmd *Command) (any, error) {
	body, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	return s.Submit(ctx, body)
}

func (s *RaftRedisServer) shardLabel() string {
	if s.ShardID == "" {
		return s.NodeID
	}
	return s.ShardID
}

func (s *RaftRedisServer) StartExecutionLoop() {
	s.executionOnce.Do(func() {
		go s.runExecutionLoop()
	})
}

func (s *RaftRedisServer) runExecutionLoop() {
	defer close(s.executionDone)
	for {
		select {
		case <-s.stopExecution:
			return
		case req, ok := <-s.commandQueue:
			if !ok {
				return
			}
			result, err := s.executeCommand(req.ctx, req.cmd, req.body)
			if req.response != nil {
				req.response <- commandResponse{result: result, err: err}
			}
		}
	}
}

func (s *RaftRedisServer) executeCommand(ctx context.Context, cmd string, body []byte) (any, error) {
	var commandReq Command
	if len(body) > 0 {
		if err := json.Unmarshal(body, &commandReq); err != nil {
			return nil, err
		}
	}
	if cmd != "" {
		commandReq.Cmd = cmd
	}
	if commandReq.Cmd == "" {
		return nil, fmt.Errorf("missing command")
	}

	if isWriteCommand(commandReq.Cmd) {
		index, term, err := s.raftNode.Propose(commandReq.Cmd, body)
		if err != nil {
			return nil, err
		}
		if err := s.raftNode.WaitApplied(ctx, index); err != nil {
			return nil, err
		}
		if applied := s.raftNode.EntryTerm(index); applied != term {
			return nil, fmt.Errorf(
				"%s was not committed: leadership changed at index %d (proposed in term %d, applied term %d)",
				commandReq.Cmd, index, term, applied)
		}
		result, applyErr, ok := s.redisStore.TakeResult(index)
		if !ok {
			return "OK", nil // result aged out of the ring; the write did commit
		}
		if applyErr != nil {
			return nil, applyErr
		}
		return result, nil
	}

	readIndex, err := s.raftNode.ReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	if readIndex >= 0 {
		if err := s.raftNode.WaitApplied(ctx, readIndex); err != nil {
			return nil, err
		}
	}
	return s.redisStore.EvalAndResponse(&commandReq)
}

func (s *RaftRedisServer) Stop() {
	if s.raftNode != nil {
		s.raftNode.Stop()
	}
	select {
	case <-s.stopExecution:
	default:
		close(s.stopExecution)
	}
}

func (s *RaftRedisServer) HandleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	reply, err := s.raftNode.RequestVote(r.Context(), &args)
	if err != nil {
		http.Error(w, fmt.Sprintf("RequestVote failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		log.Printf("handleRequestVote: encode failed: %v", err)
	}
}

func (s *RaftRedisServer) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	reply, err := s.raftNode.AppendEntries(r.Context(), &args)
	if err != nil {
		http.Error(w, fmt.Sprintf("AppendEntries failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		log.Printf("handleAppendEntries: encode failed: %v", err)
	}
}

func (s *RaftRedisServer) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
		log.Printf("handleHealth: encode failed: %v", err)
	}
}

func IsWriteCommand(cmd string) bool { return isWriteCommand(cmd) }

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "EXPIRE", "INCR",
		"SADD", "SREM", "SPOP",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMAX", "ZPOPMIN",
		"GEOADD",
		"BF_RESERVE", "BF_MADD",
		"CMS_INITBYDIM", "CMS_INITBYPROB", "CMS_INCRBY",
		"SL_ADD", "SL_DELETE",
		CmdRestore, CmdDelSlot:
		return true
	default:
		return false
	}
}

// leaderURL is this shard's group on the leader — complete but for the endpoint.
func (s *RaftRedisServer) leaderURL() (string, error) {
	leaderID := s.raftNode.GetLeaderID()
	if leaderID == "" {
		return "", fmt.Errorf("leader unknown")
	}

	addr, ok := s.peers[leaderID]
	if !ok {
		return "", fmt.Errorf("leader address not found for %s", leaderID)
	}
	return addr, nil
}

func (s *RaftRedisServer) forwardToLeader(ctx context.Context, body []byte) (*http.Response, error) {
	leaderURL, err := s.leaderURL()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderURL+"/raft/command", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func (s *RaftRedisServer) HandleCommand(w http.ResponseWriter, r *http.Request) {
	leaderID := s.raftNode.GetLeaderID()
	if leaderID == "" {
		http.Error(w, "leader unknown", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCommandBodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
		return
	}

	var commandReq Command
	_ = json.Unmarshal(body, &commandReq)
	// in case client direct call to replica node => forward to leadernode to handler
	// leader: 5001
	// replica: 5002
	// replica: 5003
	// client call 0.0.0.1:5002/command
	if leaderID != s.NodeID {
		resp, err := s.forwardToLeader(r.Context(), body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to forward to leader: %v", err), http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("[%s] failed to copy forwarded response: %v", s.NodeID, err)
		}
		return
	}

	respCh := make(chan commandResponse, 1)
	req := commandRequest{
		cmd:      "",
		body:     body,
		ctx:      r.Context(),
		response: respCh,
	}

	select {
	case s.commandQueue <- req:
	case <-r.Context().Done():
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
		return
	}

	select {
	case resp := <-respCh:
		w.Header().Set("Content-Type", "application/json")
		if resp.err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(commandHTTPResponse{
				NodeID:  s.NodeID,
				Command: commandReq.Cmd,
				Status:  "error",
				Error:   resp.err.Error(),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(commandHTTPResponse{
			NodeID:  s.NodeID,
			Command: commandReq.Cmd,
			Status:  "success",
			Result:  resp.result,
		})
	case <-r.Context().Done():
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
	}
}

func (s *RaftRedisServer) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	role := s.raftNode.GetRole()
	roleStr := "unknown"
	switch role {
	case raft.FollowerRole:
		roleStr = "follower"
	case raft.CandidateRole:
		roleStr = "candidate"
	case raft.LeaderRole:
		roleStr = "leader"
	}

	status := map[string]any{
		"node_id":   s.NodeID,
		"shard_id":  s.ShardID,
		"role":      roleStr,
		"leader_id": s.raftNode.GetLeaderID(),
		"is_leader": role == raft.LeaderRole,
		"term":      s.raftNode.GetCurrentTerm(),
		"timestamp": time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("handleStatus: encode failed: %v", err)
	}
}
