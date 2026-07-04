package cmd

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/namnv2496/go-redis-raft/raft"
	"github.com/namnv2496/go-redis-raft/redis"
	"github.com/namnv2496/go-redis-raft/redis/data_structure"
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

func getThreadCount() int {
	threads := getEnv("THREADS", "3")
	if threads == "" {
		threads = "0"
	}
	value, err := strconv.Atoi(threads)
	if err != nil || value <= 0 {
		return 4
	}
	return value
}

type ServerConfig struct {
	NodeID    string
	Port      int
	Peers     map[string]string // nodeID -> HTTP URL
	LogFile   string
	StateFile string
	DataFile  string
}

func StartRedisServer() error {
	// Read from environment variables or use defaults
	nodeID := getEnv("NODE", "node1")
	portStr := getEnv("PORT", "5000")
	peers := getEnv("PEERS", "")
	logFile := getEnv("LOGFILE", "")
	stateFile := getEnv("STATEFILE", "")
	evictPolicy := getEnv("EVICT_POLICY", "noeviction") // noeviction | lru | lfu

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
	evictStrategy := data_structure.EvictFirst
	switch evictPolicy {
	case "lru":
		evictStrategy = data_structure.EvictLRU
	case "lfu":
		evictStrategy = data_structure.EvictLFU
	}
	redisStore := redis.NewRedisStoreWithEviction(raftNode, evictStrategy)
	server, err := redis.NewRaftRedisServer(nodeID, raftNode, redisStore, peersMap)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	server.StartExecutionLoop()

	// Start HTTP server for Raft consensus
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/raft/vote", server.HandleRequestVote)
		mux.HandleFunc("/raft/append", server.HandleAppendEntries)
		mux.HandleFunc("/raft/command", server.HandleCommand)
		mux.HandleFunc("/health", server.HandleHealth)
		mux.HandleFunc("/status", server.HandleStatus)

		workerThreads := getThreadCount()
		if workerThreads > 0 {
			runtime.GOMAXPROCS(workerThreads)
			log.Printf("[%s] configuring HTTP server with %d worker threads", nodeID, workerThreads)
		}

		srv := &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		addr := srv.Addr
		log.Printf("[%s] Raft HTTP server listening on %s", nodeID, addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
