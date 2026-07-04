package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/raft"
)

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

type RaftRedisServer struct {
	NodeID        string
	raftNode      *raft.RaftNode
	redisStore    IRedisStore
	peers         map[string]string
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
	raftNode.Start()
	return &RaftRedisServer{
		NodeID:        nodeID,
		raftNode:      raftNode,
		redisStore:    redisStore,
		peers:         peers,
		kvMu:          sync.RWMutex{},
		commandQueue:  make(chan commandRequest, 256),
		stopExecution: make(chan struct{}),
		executionDone: make(chan struct{}),
	}, nil
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
		index, err := s.raftNode.Propose(commandReq.Cmd, body)
		if err != nil {
			return nil, err
		}
		if err := s.raftNode.WaitCommit(ctx, index); err != nil {
			return nil, err
		}
		return map[string]any{"node_id": s.NodeID, "result": "OK"}, nil
	}

	result, err := s.redisStore.EvalAndResponse(&commandReq)
	if err != nil {
		return nil, err
	}
	return map[string]any{"node_id": s.NodeID, "result": result}, nil
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

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "DEL", "EXPIRE", "INCR",
		"SADD", "SREM", "SPOP",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMAX", "ZPOPMIN",
		"GEOADD",
		"BF_RESERVE", "BF_MADD",
		"CMS_INITBYDIM", "CMS_INITBYPROB", "CMS_INCRBY":
		return true
	default:
		return false
	}
}

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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
		return
	}

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
		if resp.err != nil {
			http.Error(w, fmt.Sprintf("Command failed: %v", resp.err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp.result); err != nil {
			log.Printf("[%s] Failed to encode response: %v", s.NodeID, err)
		}
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
