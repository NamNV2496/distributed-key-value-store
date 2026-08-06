package proxy

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/namnv2496/go-redis-raft/cmd"
	"github.com/namnv2496/go-redis-raft/proxy"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "proxy",
	Short: "Route client commands to the shard that owns the key, hosting no shard itself",
	RunE: func(*cobra.Command, []string) error {
		return Start()
	},
}

func Start() error {
	port, err := cmd.Port("PORT", 6000)
	if err != nil {
		return err
	}
	seeds := strings.Split(cmd.Get("SEEDS", cmd.Get("PEERS", "")), ",")

	refresh, err := time.ParseDuration(cmd.Get("TOPOLOGY_REFRESH", "5s"))
	if err != nil {
		return fmt.Errorf("TOPOLOGY_REFRESH must be a duration such as 5s, got %q: %w",
			cmd.Get("TOPOLOGY_REFRESH", ""), err)
	}
	if cmd.DashboardPortRequested() {
		return fmt.Errorf(
			"the proxy does not serve a dashboard; run the dashboard command instead:\n" +
				"  SEEDS=$SEEDS PORT=<port> raft-redis dashboard")
	}

	proxyID := cmd.Get("PROXY_ID", cmd.Get("NODE", "proxy"))
	router, err := proxy.New(proxy.Config{
		ID:              proxyID,
		Seeds:           seeds,
		TopologyPath:    cmd.TopologyPath(cmd.Get("DATA_DIR", ""), proxyID),
		RefreshInterval: refresh,
	})
	if err != nil {
		return fmt.Errorf("%w (set SEEDS=http://node1:5000,http://node4:5000)", err)
	}
	defer router.Stop()

	mux := http.NewServeMux()
	router.Routes(mux)

	go cmd.Serve(proxyID, "proxy", port, mux)
	log.Printf("[%s] routing for seeds %s, refreshing every %s",
		proxyID, strings.Join(router.Seeds(), ", "), refresh)
	if topo := router.Topology(); topo != nil {
		log.Printf("[%s] topology v%d: %d slots over %d shard(s) %v",
			proxyID, topo.Version, topo.SlotCount, len(topo.Shards), topo.ShardIDs())
	}
	log.Printf("[%s] POST http://localhost:%d/kv to route a command", proxyID, port)

	log.Printf("[%s] received signal: %v", proxyID, cmd.WaitForSignal())
	return nil
}
