package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/namnv2496/go-redis-raft/raft"
	"github.com/spf13/cobra"
)

var RedisCmd = &cobra.Command{
	Use:   "redis",
	Short: "Start Raft node server (consensus only)",
	RunE: func(cmd *cobra.Command, args []string) error {
		StartRedisServer()
		return nil
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
	config     ServerConfig
	raftNode   *raft.RaftNode
	httpServer *http.Server
	mu         sync.RWMutex
}

func StartRedisServer() {
	// Read from environment variables or use defaults
	nodeID := getEnv("NODE", "node1")
	portStr := getEnv("PORT", "5000")
	peers := getEnv("PEERS", "")
	logFile := getEnv("LOGFILE", "")
	stateFile := getEnv("STATEFILE", "")

	// Parse port
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	if port == 0 {
		port = 5000
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
		for _, peer := range strings.Split(peers, ",") {
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

	config := ServerConfig{
		NodeID:    nodeID,
		Port:      port,
		Peers:     peersMap,
		LogFile:   logFile,
		StateFile: stateFile,
	}

	log.Printf("Starting Raft node server (consensus only)")
	log.Printf("  Node ID: %s", config.NodeID)
	log.Printf("  Port: %d", config.Port)
	log.Printf("  Peers: %v", config.Peers)

	server, err := NewRaftRedisServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start HTTP server for Raft consensus
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/raft/vote", server.handleRequestVote)
		mux.HandleFunc("/raft/append", server.handleAppendEntries)
		mux.HandleFunc("/health", server.handleHealth)
		mux.HandleFunc("/status", server.handleStatus)

		addr := fmt.Sprintf(":%d", config.Port)
		log.Printf("[%s] Raft HTTP server listening on %s", config.NodeID, addr)
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] HTTP server error: %v", config.NodeID, err)
		}
	}()

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

func NewRaftRedisServer(config ServerConfig) (*RaftRedisServer, error) {
	s := &RaftRedisServer{
		config: config,
	}

	// Create Raft node
	raftConfig := raft.Config{
		NodeID:    config.NodeID,
		Peers:     config.Peers,
		LogFile:   config.LogFile,
		StateFile: config.StateFile,
		OnStateChange: func(role raft.NodeRole) {
			roleStr := "Unknown"
			switch role {
			case raft.FollowerRole:
				roleStr = "Follower"
			case raft.CandidateRole:
				roleStr = "Candidate"
			case raft.LeaderRole:
				roleStr = "Leader"
				log.Printf("[%s] ⭐ Became LEADER\n", config.NodeID)
			}
			log.Printf("[%s] Node role changed to: %s\n", config.NodeID, roleStr)
		},
	}

	raftNode, err := raft.NewRaftNode(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	s.raftNode = raftNode

	// Start the Raft node
	raftNode.Start()

	return s, nil
}

// Stop gracefully stops the server
func (s *RaftRedisServer) Stop() {
	if s.raftNode != nil {
		s.raftNode.Stop()
	}
	if s.httpServer != nil {
		s.httpServer.Close()
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
	json.NewEncoder(w).Encode(reply)
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
	json.NewEncoder(w).Encode(reply)
}

// handleHealth handles the /health endpoint
func (s *RaftRedisServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
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

	status := map[string]interface{}{
		"node_id":   s.config.NodeID,
		"role":      roleStr,
		"leader_id": s.raftNode.GetLeaderID(),
		"is_leader": role == raft.LeaderRole,
		"term":      s.raftNode.GetCurrentTerm(),
		"timestamp": time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
