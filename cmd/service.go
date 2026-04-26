package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// Command is the canonical write operation sent through the Raft log.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	TTLMs int64  `json:"ttl_ms,omitempty"`
}

var ServiceCmd = &cobra.Command{
	Use:   "service",
	Short: "Start REST API service with Raft client",
	RunE: func(cmd *cobra.Command, args []string) error {
		StartService()
		return nil
	},
}

// ServiceConfig holds the service configuration
type ServiceConfig struct {
	NodeID   string
	Port     int
	Peers    map[string]string // nodeID -> HTTP URL
	DataFile string
}

// ServiceServer represents the REST API service
type ServiceServer struct {
	config     ServiceConfig
	httpServer *http.Server
}

func StartService() {
	// Read from environment variables or use defaults
	nodeID := getEnv("NODE", "service")
	portStr := getEnv("PORT", "8000")
	peers := getEnv("PEERS", "")
	dataFile := getEnv("DATAFILE", "")

	// Parse port
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	if port == 0 {
		port = 8000
	}

	// Default data file
	if dataFile == "" {
		dataFile = fmt.Sprintf(".data-%s.json", nodeID)
	}

	// Parse peers - these are the Raft node URLs
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

	config := ServiceConfig{
		NodeID:   nodeID,
		Port:     port,
		Peers:    peersMap,
		DataFile: dataFile,
	}

	log.Printf("Starting REST API service")
	log.Printf("  Node ID: %s", config.NodeID)
	log.Printf("  Port: %d", config.Port)
	log.Printf("  Raft Peers: %v", config.Peers)

	server, err := NewServiceServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Redis endpoints - these go through Raft consensus
	mux.HandleFunc("/redis/set", server.handleRedisSet)
	mux.HandleFunc("/redis/get", server.handleRedisGet)
	mux.HandleFunc("/redis/del", server.handleRedisDelete)

	// Status endpoint - shows local state
	mux.HandleFunc("/status", server.handleStatus)
	mux.HandleFunc("/health", server.handleHealth)

	// Raft cluster status - query all nodes to find leader
	mux.HandleFunc("/cluster/status", server.handleClusterStatus)

	addr := fmt.Sprintf(":%d", config.Port)
	server.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		server.Stop()
		os.Exit(0)
	}()

	log.Printf("[%s] Starting HTTP server on %s\n", config.NodeID, addr)
	if err := server.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// NewServiceServer creates a new service server
func NewServiceServer(config ServiceConfig) (*ServiceServer, error) {
	s := &ServiceServer{
		config: config,
	}
	return s, nil
}

// Stop gracefully stops the server
func (s *ServiceServer) Stop() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

// findLeader queries all Raft nodes to find the current leader
func (s *ServiceServer) findLeader() (leaderID string, leaderURL string, err error) {
	for nodeID, url := range s.config.Peers {
		resp, err := http.Get(url + "/status")
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		var status map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			continue
		}

		if isLeader, ok := status["is_leader"].(bool); ok && isLeader {
			return nodeID, url, nil
		}
	}
	return "", "", fmt.Errorf("no leader found")
}

// forwardToLeader forwards a request to the current leader
func (s *ServiceServer) forwardToLeader(endpoint string, data []byte) (*http.Response, error) {
	_, leaderURL, err := s.findLeader()
	if err != nil {
		return nil, err
	}

	url := leaderURL + endpoint
	return http.Post(url, "application/json", strings.NewReader(string(data)))
}

// SetRequest represents a SET request
type SetRequest struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	ExpireMs int64  `json:"expire_ms"`
}

// GetRequest represents a GET request
type GetRequest struct {
	Key string `json:"key"`
}

// GetResponse represents a GET response
type GetResponse struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

// DeleteRequest represents a DELETE request
type DeleteRequest struct {
	Key string `json:"key"`
}

// LogEntry represents a Raft log entry
type LogEntry struct {
	Term  int64  `json:"term"`
	Index int64  `json:"index"`
	Data  []byte `json:"data"`
}

// handleRedisSet handles the /redis/set endpoint
func (s *ServiceServer) handleRedisSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	cmd := Command{Op: "set", Key: req.Key, Value: req.Value, TTLMs: req.ExpireMs}
	data, _ := json.Marshal(cmd)

	resp, err := s.forwardToLeader("/raft/command", data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward to leader: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		http.Error(w, string(body), resp.StatusCode)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// handleRedisGet handles the /redis/get endpoint
func (s *ServiceServer) handleRedisGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	data, _ := json.Marshal(req)
	resp, err := s.forwardToLeader("/raft/get", data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward to leader: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// handleRedisDelete handles the /redis/del endpoint
func (s *ServiceServer) handleRedisDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	cmd := Command{Op: "del", Key: req.Key}
	data, _ := json.Marshal(cmd)

	resp, err := s.forwardToLeader("/raft/command", data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward to leader: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		http.Error(w, string(body), resp.StatusCode)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

// handleStatus handles the /status endpoint
func (s *ServiceServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	leaderID, _, _ := s.findLeader()

	status := map[string]interface{}{
		"node_id":   s.config.NodeID,
		"role":      "service",
		"leader_id": leaderID,
		"timestamp": time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleHealth handles the /health endpoint
func (s *ServiceServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

// handleClusterStatus handles the /cluster/status endpoint
func (s *ServiceServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type NodeStatus struct {
		NodeID   string `json:"node_id"`
		Role     string `json:"role"`
		LeaderID string `json:"leader_id"`
		IsLeader bool   `json:"is_leader"`
		Term     int64  `json:"term"`
		URL      string `json:"url"`
	}

	nodes := make([]NodeStatus, 0)
	for nodeID, url := range s.config.Peers {
		resp, err := http.Get(url + "/status")
		if err != nil {
			nodes = append(nodes, NodeStatus{NodeID: nodeID, Role: "unreachable", URL: url})
			continue
		}
		defer resp.Body.Close()

		var status map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			nodes = append(nodes, NodeStatus{NodeID: nodeID, Role: "error", URL: url})
			continue
		}

		role, _ := status["role"].(string)
		leaderID, _ := status["leader_id"].(string)
		isLeader, _ := status["is_leader"].(bool)
		term, _ := status["term"].(float64)

		nodes = append(nodes, NodeStatus{
			NodeID:   nodeID,
			Role:     role,
			LeaderID: leaderID,
			IsLeader: isLeader,
			Term:     int64(term),
			URL:      url,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"nodes": nodes})
}
