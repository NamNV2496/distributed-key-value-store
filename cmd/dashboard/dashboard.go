package dashboard

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/namnv2496/go-redis-raft/cmd"
	"github.com/namnv2496/go-redis-raft/dashboard"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Serve the shard dashboard for a cluster this process is not part of",
	RunE: func(*cobra.Command, []string) error {
		return Start()
	},
}

func Start() error {
	port, err := cmd.Port("PORT", 8080)
	if err != nil {
		return err
	}
	seeds := strings.Split(cmd.Get("SEEDS", cmd.Get("PEERS", "")), ",")
	watcher, err := dashboard.New(seeds)
	if err != nil {
		return fmt.Errorf("%w (set SEEDS=http://node1:5000,http://node4:5000)", err)
	}

	mux := http.NewServeMux()
	watcher.Routes(mux)

	go cmd.Serve("dashboard", "dashboard", port, mux)
	log.Printf("[dashboard] watching %s", strings.Join(watcher.Seeds(), ", "))
	log.Printf("[dashboard] open http://localhost:%d/dashboard/", port)

	log.Printf("[dashboard] received signal: %v", cmd.WaitForSignal())
	return nil
}
