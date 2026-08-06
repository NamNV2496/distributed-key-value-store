package redis

import (
	"fmt"
	"log"
	"net/http"
	"runtime"

	"github.com/namnv2496/go-redis-raft/cmd"
	"github.com/namnv2496/go-redis-raft/shard"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "redis",
	Short: "Start Raft node server (consensus only)",
	RunE: func(*cobra.Command, []string) error {
		return Start()
	},
}

func Start() error {
	node := raftOptionsFromEnv()

	clusterOpts, err := clusterOptionsFromEnv()
	if err != nil {
		return err
	}

	shardOpts, err := shardOptionsFromEnv()
	if err != nil {
		return err
	}
	if cmd.DashboardPortRequested() {
		return fmt.Errorf(
			"a node does not serve a dashboard; run the dashboard command instead:\n" +
				"  SEEDS=$ADVERTISE PORT=<port> raft-redis dashboard")
	}

	if shardOpts.Threads > 0 {
		runtime.GOMAXPROCS(shardOpts.Threads)
		log.Printf("[%s] configuring HTTP server with %d worker threads", node.NodeID, shardOpts.Threads)
	}

	bootstrap, err := clusterOpts.bootstrap(node.Peers)
	if err != nil {
		return fmt.Errorf("failed to build cluster topology: %w", err)
	}

	manager, err := shard.New(shard.Config{
		NodeID:        node.NodeID,
		Advertise:     node.Advertise,
		DataDir:       node.DataDir,
		EvictStrategy: shard.EvictStrategyFromPolicy(shardOpts.EvictPolicy),
		Bootstrap:     bootstrap,
		TopologyPath:  cmd.TopologyPath(node.DataDir, node.NodeID),
	})
	if err != nil {
		return fmt.Errorf("failed to start shard manager: %w", err)
	}
	defer manager.Stop()

	topo := manager.Topology()
	log.Printf("[%s] cluster topology v%d: %d slots over %d shard(s) %v; hosting %v",
		node.NodeID, topo.Version, topo.SlotCount, len(topo.Shards), topo.ShardIDs(), manager.LocalShards())

	mux := http.NewServeMux()
	manager.Routes(mux)
	go cmd.Serve(node.NodeID, "Raft", shardOpts.Port, mux)

	log.Printf("[%s] received signal: %v", node.NodeID, cmd.WaitForSignal())
	return nil
}
