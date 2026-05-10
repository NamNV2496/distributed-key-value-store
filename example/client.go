/*
Curl examples for every supported command.
All commands POST to /redis with body: {"cmd":"<CMD>","args":{...}}
The client auto-discovers the leader; you may POST to any node.

────────────────────────────────────────────── String / Key ──────────────────

  # PING
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"PING"}'

  # PING with message
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"PING","args":{"message":"hello"}}'

  # SET
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SET","args":{"key":"foo","value":"bar"}}'

  # SET with TTL (seconds)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SET","args":{"key":"foo","value":"bar","ttl":"60"}}'

  # GET
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GET","args":{"key":"foo"}}'

  # TTL  (-1=no expiry, -2=key not found)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"TTL","args":{"key":"foo"}}'

  # DEL
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"DEL","args":{"key":"foo"}}'

  # EXPIRE (seconds)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"EXPIRE","args":{"key":"foo","ttl":"120"}}'

  # INCR
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"INCR","args":{"key":"counter"}}'

────────────────────────────────────────────────────────────── Set ───────────

  # SADD
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SADD","args":{"key":"myset","value":"alice"}}'

  # SREM
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SREM","args":{"key":"myset","value":"alice"}}'

  # SCARD
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SCARD","args":{"key":"myset"}}'

  # SMEMBERS
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SMEMBERS","args":{"key":"myset"}}'

  # SISMEMBER
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SISMEMBER","args":{"key":"myset","value":"alice"}}'

  # SMISMEMBER
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SMISMEMBER","args":{"key":"myset","value":"alice"}}'

  # SRAND (1 random member)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SRAND","args":{"key":"myset"}}'

  # SRAND (3 random members)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SRAND","args":{"key":"myset","value":"3"}}'

  # SPOP (1 member)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SPOP","args":{"key":"myset"}}'

  # SPOP (2 members)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"SPOP","args":{"key":"myset","value":"2"}}'

──────────────────────────────────────────────────────── Sorted Set ──────────

  # ZADD
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZADD","args":{"key":"board","score":"100","member":"alice"}}'

  # ZADD NX  (only if member does not exist)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZADD","args":{"key":"board","score":"200","member":"bob","nx":"true"}}'

  # ZADD XX  (only if member already exists)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZADD","args":{"key":"board","score":"300","member":"alice","xx":"true"}}'

  # ZRANK  (0-based rank, ascending)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZRANK","args":{"key":"board","member":"alice"}}'

  # ZSCORE
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZSCORE","args":{"key":"board","member":"alice"}}'

  # ZCARD
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZCARD","args":{"key":"board"}}'

  # ZREM  (extra args beyond "key" are treated as members to remove)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"ZREM","args":{"key":"board","m1":"alice","m2":"bob"}}'

──────────────────────────────────────────────────────────────── Geo ─────────

  # GEOADD
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEOADD","args":{"key":"cities","longitude":"13.361389","latitude":"38.115556","member":"Palermo"}}'

  # GEODIST  (unit: m | km | ft | mi)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEODIST","args":{"key":"cities","member1":"Palermo","member2":"Catania","unit":"km"}}'

  # GEOHASH  (extra args beyond "key" are treated as members)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEOHASH","args":{"key":"cities","m1":"Palermo","m2":"Catania"}}'

  # GEOPOS  (extra args beyond "key" are treated as members)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEOPOS","args":{"key":"cities","m1":"Palermo"}}'

  # GEOSEARCH from member  (radius in metres)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEOSEARCH","args":{"key":"cities","frommember":"Palermo","radius":"200000"}}'

  # GEOSEARCH from lon/lat
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"GEOSEARCH","args":{"key":"cities","fromlonlat_lon":"15","fromlonlat_lat":"37","radius":"200000"}}'

──────────────────────────────────────────────────────── Bloom Filter ────────

  # BF_RESERVE  (optional: add "expansion":"EXPANSION","growthRate":"2")
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"BF_RESERVE","args":{"key":"bf","errRate":"0.01","capacity":"10000"}}'

  # BF_MADD  (items keyed "1","2",... up to N)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"BF_MADD","args":{"key":"bf","1":"foo","2":"bar","3":"baz"}}'

  # BF_EXISTS
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"BF_EXISTS","args":{"key":"bf","item":"foo"}}'

  # BF_MEXISTS  (items keyed "1","2",... up to N)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"BF_MEXISTS","args":{"key":"bf","1":"foo","2":"missing"}}'

  # BF_INFO
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"BF_INFO","args":{"key":"bf"}}'

──────────────────────────────────────────────────── Count-Min Sketch ────────

  # CMS_INITBYDIM  (explicit width × height grid)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"CMS_INITBYDIM","args":{"key":"cms","width":"2000","height":"7"}}'

  # CMS_INITBYPROB  (derive grid from error-rate + confidence)
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"CMS_INITBYPROB","args":{"key":"cms2","errRate":"0.001","probability":"0.999"}}'

  # CMS_INCRBY
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"CMS_INCRBY","args":{"key":"cms","value":"1"}}'

  # CMS_QUERY
  curl -s -X POST http://localhost:8000/redis \
    -H 'Content-Type: application/json' \
    -d '{"cmd":"CMS_QUERY","args":{"key":"cms"}}'

──────────────────────────────────────────────────────────── Cluster ─────────

  # Node status
  curl -s http://localhost:8000/status

  # Cluster status (queries all peers)
  curl -s http://localhost:8000/cluster/status
*/
package main

import (
	"bytes"
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
)

// Command matches the redis.Command struct on the server side.
type Command struct {
	Cmd  string            `json:"cmd"`
	Args map[string]string `json:"args,omitempty"`
}

type ServiceConfig struct {
	NodeID string
	Port   int
	Peers  map[string]string // nodeID -> HTTP URL
}

type ServiceServer struct {
	config     ServiceConfig
	httpServer *http.Server
}

func main() {
	nodeID := getEnv("NODE", "service")
	portStr := getEnv("PORT", "8000")
	peers := getEnv("PEERS", "")

	var port int
	fmt.Sscanf(portStr, "%d", &port)
	if port == 0 {
		port = 8000
	}

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

	config := ServiceConfig{
		NodeID: nodeID,
		Port:   port,
		Peers:  peersMap,
	}

	log.Printf("Starting REST API client-service")
	log.Printf("  Node ID: %s", config.NodeID)
	log.Printf("  Port:    %d", config.Port)
	log.Printf("  Peers:   %v", config.Peers)

	server := &ServiceServer{config: config}

	mux := http.NewServeMux()
	mux.HandleFunc("/redis", server.handleRedis)
	mux.HandleFunc("/status", server.handleStatus)
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/cluster/status", server.handleClusterStatus)

	addr := fmt.Sprintf(":%d", config.Port)
	server.httpServer = &http.Server{Addr: addr, Handler: mux}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		server.Stop()
		os.Exit(0)
	}()

	log.Printf("[%s] HTTP server listening on %s", config.NodeID, addr)
	if err := server.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}

func (s *ServiceServer) Stop() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

// handleRedis is the universal endpoint. It accepts any Command and forwards
// it to the Raft leader's /raft/command endpoint.
//
//	POST /redis
//	{"cmd":"SET","args":{"key":"foo","value":"bar"}}
func (s *ServiceServer) handleRedis(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate JSON and extract the command name for logging.
	var cmd Command
	if err := json.Unmarshal(body, &cmd); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}
	if cmd.Cmd == "" {
		http.Error(w, `"cmd" field is required`, http.StatusBadRequest)
		return
	}

	resp, err := s.forwardToLeader("/raft/command", body)
	if err != nil {
		http.Error(w, fmt.Sprintf("forward to leader: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[%s] copy response for %s: %v", s.config.NodeID, cmd.Cmd, err)
	}
}

// findLeader queries all known peers and returns the URL of the current leader.
func (s *ServiceServer) findLeader() (string, error) {
	for _, url := range s.config.Peers {
		resp, err := http.Get(url + "/status")
		if err != nil {
			continue
		}
		var status map[string]any
		decodeErr := json.NewDecoder(resp.Body).Decode(&status)
		_ = resp.Body.Close()
		if decodeErr != nil {
			continue
		}
		if isLeader, ok := status["is_leader"].(bool); ok && isLeader {
			return url, nil
		}
	}
	return "", fmt.Errorf("no leader found among peers")
}

func (s *ServiceServer) forwardToLeader(endpoint string, body []byte) (*http.Response, error) {
	leaderURL, err := s.findLeader()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, leaderURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func (s *ServiceServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	leaderURL, err := s.findLeader()
	leaderInfo := leaderURL
	if err != nil {
		leaderInfo = "unknown"
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"node_id":    s.config.NodeID,
		"role":       "client-service",
		"leader_url": leaderInfo,
		"timestamp":  time.Now().Unix(),
	}); err != nil {
		log.Printf("handleStatus encode: %v", err)
	}
}

func (s *ServiceServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
		log.Printf("handleHealth encode: %v", err)
	}
}

func (s *ServiceServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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

	nodes := make([]NodeStatus, 0, len(s.config.Peers))
	for nodeID, url := range s.config.Peers {
		resp, err := http.Get(url + "/status")
		if err != nil {
			nodes = append(nodes, NodeStatus{NodeID: nodeID, Role: "unreachable", URL: url})
			continue
		}
		var status map[string]any
		decodeErr := json.NewDecoder(resp.Body).Decode(&status)
		_ = resp.Body.Close()
		if decodeErr != nil {
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
	if err := json.NewEncoder(w).Encode(map[string]any{"nodes": nodes}); err != nil {
		log.Printf("handleClusterStatus encode: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
