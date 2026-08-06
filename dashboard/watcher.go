package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/status"
)

type Watcher struct {
	seeds      []string
	httpClient *http.Client

	mu       sync.RWMutex
	topo     *cluster.Topology
	lastSeed string
}

func New(seeds []string) (*Watcher, error) {
	cleaned := routing.CleanSeeds(seeds)
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("dashboard: at least one seed node URL is required")
	}
	return &Watcher{
		seeds:      cleaned,
		httpClient: &http.Client{Timeout: status.ProbeTimeout},
	}, nil
}

func (w *Watcher) Seeds() []string { return w.seeds }

func (w *Watcher) Routes(mux *http.ServeMux) {
	registerUI(mux)

	mux.HandleFunc("/cluster/status", w.HandleClusterStatus)
	mux.HandleFunc("/cluster/locate", w.HandleLocate)
	mux.HandleFunc("/cluster/topology", routing.ReadOnly(w.HandleTopology))
	mux.HandleFunc("/health", w.HandleHealth)
}

func (w *Watcher) refresh(ctx context.Context) (*cluster.Topology, string, error) {
	var errs []string
	for _, seed := range w.seeds {
		topo, err := routing.FetchTopologyFrom(ctx, w.httpClient, seed)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", seed, err))
			continue
		}
		w.mu.Lock()
		w.topo, w.lastSeed = topo, seed
		w.mu.Unlock()
		return topo, seed, nil
	}

	w.mu.RLock()
	cached, seed := w.topo, w.lastSeed
	w.mu.RUnlock()
	if cached != nil {
		return cached, seed, nil
	}
	return nil, "", fmt.Errorf("no seed answered: %s", strings.Join(errs, "; "))
}

func (w *Watcher) HandleClusterStatus(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topo, seed, err := w.refresh(r.Context())
	if err != nil {
		http.Error(rw, err.Error(), http.StatusServiceUnavailable)
		return
	}

	probes := status.Prober{Client: w.httpClient}.Probe(r.Context(), cluster.NodeAddresses(topo))
	routing.WriteJSON(rw, http.StatusOK, status.ClusterStatus{
		ServedBy:    "dashboard",
		Source:      seed,
		GeneratedAt: time.Now().UnixMilli(),
		Topology:    status.Summarise(topo),
		Shards:      status.BuildShardViews(topo, probes),
		Nodes:       probes,
		Migrations:  topo.Migrations,
	})
}

func (w *Watcher) HandleLocate(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(rw, "missing 'key' query parameter", http.StatusBadRequest)
		return
	}
	topo, _, err := w.refresh(r.Context())
	if err != nil {
		http.Error(rw, err.Error(), http.StatusServiceUnavailable)
		return
	}

	slot, shard := topo.Locate(key)
	payload := map[string]any{
		"key":              key,
		"hash_tag":         cluster.HashTag(key),
		"slot":             slot,
		"slot_count":       topo.SlotCount,
		"topology_version": topo.Version,
	}
	if shard != nil {
		payload["shard"] = shard.ID
		payload["nodes"] = shard.Members
	}
	if mig := topo.MigrationFor(slot); mig != nil {
		payload["migrating"] = mig
	}
	routing.WriteJSON(rw, http.StatusOK, payload)
}

func (w *Watcher) HandleTopology(rw http.ResponseWriter, r *http.Request) {
	topo, _, err := w.refresh(r.Context())
	if err != nil {
		http.Error(rw, err.Error(), http.StatusServiceUnavailable)
		return
	}
	routing.WriteJSON(rw, http.StatusOK, topo)
}

func (w *Watcher) HandleHealth(rw http.ResponseWriter, r *http.Request) {
	w.mu.RLock()
	topo, seed := w.topo, w.lastSeed
	w.mu.RUnlock()

	payload := map[string]any{"status": "healthy", "role": "dashboard", "seeds": w.seeds}
	if topo != nil {
		payload["topology_version"] = topo.Version
		payload["source"] = seed
	}
	routing.WriteJSON(rw, http.StatusOK, payload)
}
