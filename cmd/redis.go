package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/namnv2496/go-redis-raft/raft"
	"github.com/namnv2496/go-redis-raft/redis"
	"github.com/spf13/cobra"
)

var RedisCmd = &cobra.Command{
	Use:   "redis",
	Short: "Start Raft node server (consensus only)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return StartRedisServer()
	},
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

type ServerConfig struct {
	NodeID    string
	Port      int
	Peers     map[string]string // nodeID -> HTTP URL
	LogFile   string
	StateFile string
	DataFile  string
}

type RaftRedisServer struct {
	NodeID     string
	raftNode   *raft.RaftNode
	redisStore redis.IRedisStore
	peers      map[string]string
	kvMu       sync.RWMutex
}

func StartRedisServer() error {
	// Read from environment variables or use defaults
	nodeID := getEnv("NODE", "node1")
	portStr := getEnv("PORT", "5000")
	peers := getEnv("PEERS", "")
	logFile := getEnv("LOGFILE", "")
	stateFile := getEnv("STATEFILE", "")

	// Parse port
	var port int
	_, err := fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		return err
	}

	// Default file paths
	if logFile == "" {
		logFile = fmt.Sprintf(".raft-%s.log", nodeID)
	}
	if stateFile == "" {
		stateFile = fmt.Sprintf(".raft-%s.state", nodeID)
	}

	// Parse peers
	peersMap := make(map[string]string)
	if peers != "" {
		for peer := range strings.SplitSeq(peers, ",") {
			if peer == "" {
				continue
			}
			parts := strings.Split(peer, "=")
			if len(parts) == 2 {
				peersMap[parts[0]] = parts[1]
			}
		}
	}
	// Always add ourselves - use Docker network hostname
	peersMap[nodeID] = fmt.Sprintf("http://%s:5000", nodeID)
	// Create Raft node
	raftConfig := raft.Config{
		NodeID:    nodeID,
		Peers:     peersMap,
		LogFile:   logFile,
		StateFile: stateFile,
		OnStateChange: func(role raft.NodeRole) {
			roleStr := "Unknown"
			switch role {
			case raft.FollowerRole:
				roleStr = "Follower"
			case raft.CandidateRole:
				roleStr = "Candidate"
			case raft.LeaderRole:
				roleStr = "Leader"
				log.Printf("[%s] ⭐ Became LEADER\n", nodeID)
			}
			log.Printf("[%s] Node role changed to: %s\n", nodeID, roleStr)
		},
	}
	raftNode, err := raft.NewRaftNode(raftConfig)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %w", err)
	}
	redisStore := redis.NewRedisStore(raftNode)
	server, err := NewRaftRedisServer(nodeID, raftNode, redisStore, peersMap)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start HTTP server for Raft consensus
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/raft/vote", server.handleRequestVote)
		mux.HandleFunc("/raft/append", server.handleAppendEntries)
		mux.HandleFunc("/raft/command", server.handleCommand)
		mux.HandleFunc("/health", server.handleHealth)
		mux.HandleFunc("/status", server.handleStatus)

		addr := fmt.Sprintf(":%d", port)
		log.Printf("[%s] Raft HTTP server listening on %s", nodeID, addr)
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] HTTP server error: %v", nodeID, err)
		}
	}()
	go redisStore.RunApplyLoop()
	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		server.Stop()
		os.Exit(0)
	}()

	// Keep running
	select {}
}

func NewRaftRedisServer(
	nodeID string,
	raftNode *raft.RaftNode,
	redisStore redis.IRedisStore,
	peers map[string]string,
) (*RaftRedisServer, error) {
	raftNode.Start()
	return &RaftRedisServer{
		NodeID:     nodeID,
		raftNode:   raftNode,
		redisStore: redisStore,
		peers:      peers,
		kvMu:       sync.RWMutex{},
	}, nil
}

// Stop gracefully stops the server
func (s *RaftRedisServer) Stop() {
	if s.raftNode != nil {
		s.raftNode.Stop()
	}
}

func (s *RaftRedisServer) handleRequestVote(w http.ResponseWriter, r *http.Request) {
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

// handleAppendEntries handles the /raft/append endpoint
func (s *RaftRedisServer) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
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

// handleHealth handles the /health endpoint
func (s *RaftRedisServer) handleHealth(w http.ResponseWriter, r *http.Request) {
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
	case "SET", "DEL", "EXPIRE", "INCR", "ZADD", "ZREM", "GEOADD":
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

func (s *RaftRedisServer) handleCommand(w http.ResponseWriter, r *http.Request) {
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

	var commandReq redis.Command
	if err := json.Unmarshal(body, &commandReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if isWriteCommand(commandReq.Cmd) {
		index, err := s.raftNode.Propose(commandReq.Cmd, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to propose command: %v", err), http.StatusInternalServerError)
			return
		}

		if err := s.raftNode.WaitCommit(r.Context(), index); err != nil {
			http.Error(w, fmt.Sprintf("Failed to wait for command commit: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"node_id": s.NodeID,
			"result":  "OK",
		}); err != nil {
			log.Printf("[%s] Failed to encode response: %v", s.NodeID, err)
		}
		return
	}

	result, err := s.redisStore.EvalAndResponse(&commandReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Command failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"node_id": s.NodeID,
		"result":  result,
	}); err != nil {
		log.Printf("[%s] Failed to encode response: %v", s.NodeID, err)
	}
}

// handleStatus handles the /status endpoint
func (s *RaftRedisServer) handleStatus(w http.ResponseWriter, r *http.Request) {
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
