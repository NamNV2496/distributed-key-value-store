package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/redis"
	"github.com/namnv2496/go-redis-raft/status"
)

type Proxy struct {
	id           string
	seeds        []string
	topo         *cluster.Store
	peer         *routing.PeerClient
	httpClient   *http.Client
	refreshEvery time.Duration
	mu           sync.RWMutex
	preferred    map[string]string
	lastSeed     string
	stop         chan struct{}
	stopOnce     sync.Once
}

type Config struct {
	ID              string
	Seeds           []string
	TopologyPath    string
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
}

const (
	defaultRefreshInterval = 5 * time.Second
	defaultRequestTimeout  = 15 * time.Second
)

func New(cfg Config) (*Proxy, error) {
	seeds := routing.CleanSeeds(cfg.Seeds)
	if len(seeds) == 0 {
		return nil, fmt.Errorf("proxy: at least one seed node URL is required")
	}
	if cfg.ID == "" {
		cfg.ID = "proxy"
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = defaultRefreshInterval
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}

	client := &http.Client{Timeout: cfg.RequestTimeout}
	initial, seed := seedTopology(cfg.ID, client, seeds, cfg.RequestTimeout)
	if initial != nil {
		if err := initial.Save(cfg.TopologyPath); err != nil {
			log.Printf("[%s] could not cache topology at %s: %v", cfg.ID, cfg.TopologyPath, err)
		}
	} else if cached, err := cluster.LoadTopology(cfg.TopologyPath); err != nil {
		log.Printf("[%s] ignoring unreadable cached topology at %s: %v", cfg.ID, cfg.TopologyPath, err)
	} else if cached != nil {
		log.Printf("[%s] no seed answered; starting on the cached topology v%d", cfg.ID, cached.Version)
		initial = cached
	} else {
		log.Printf("[%s] no seed answered and no cached topology; routing starts once one does", cfg.ID)
	}

	p := &Proxy{
		id:           cfg.ID,
		seeds:        seeds,
		topo:         cluster.NewStore(initial, cfg.TopologyPath),
		httpClient:   client,
		refreshEvery: cfg.RefreshInterval,
		preferred:    make(map[string]string),
		lastSeed:     seed,
		stop:         make(chan struct{}),
	}
	p.peer = routing.NewPeerClient(p.id, p.topo, p.httpClient, p.install)

	if initial != nil {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
		p.refreshLeaders(ctx, initial)
		cancel()
	}

	go p.refreshLoop()
	return p, nil
}

// seedTopology reads the topology from the first seed that answers, and reports
// which one that was.
func seedTopology(id string, client *http.Client, seeds []string, timeout time.Duration) (*cluster.Topology, string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for _, seed := range seeds {
		topo, err := routing.FetchTopologyFrom(ctx, client, seed)
		if err != nil {
			log.Printf("[%s] seed %s did not answer: %v", id, seed, err)
			continue
		}
		return topo, seed
	}
	return nil, ""
}

func (p *Proxy) ID() string      { return p.id }
func (p *Proxy) Seeds() []string { return p.seeds }

// Topology is the snapshot the proxy is currently routing by, or nil before the
// first successful read.
func (p *Proxy) Topology() *cluster.Topology { return p.topo.Get() }

func (p *Proxy) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
}

func (p *Proxy) install(next *cluster.Topology) (bool, error) {
	return p.topo.Install(next)
}

func (p *Proxy) refreshLoop() {
	ticker := time.NewTicker(p.refreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), p.refreshEvery)
			if err := p.Refresh(ctx); err != nil {
				log.Printf("[%s] topology refresh failed: %v", p.id, err)
			}
			cancel()
		}
	}
}

func (p *Proxy) Refresh(ctx context.Context) error {
	var errs []error
	for _, base := range p.sources() {
		topo, err := p.peer.FetchTopology(ctx, base)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", base, err))
			continue
		}
		if _, err := p.install(topo); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", base, err))
			continue
		}
		p.mu.Lock()
		p.lastSeed = base
		p.mu.Unlock()

		p.refreshLeaders(ctx, p.topo.Get())
		return nil
	}
	return errors.Join(errs...)
}

func (p *Proxy) sources() []string {
	out := append([]string(nil), p.seeds...)
	seen := make(map[string]bool, len(out))
	for _, s := range out {
		seen[s] = true
	}
	if topo := p.topo.Get(); topo != nil {
		for _, addr := range cluster.NodeAddresses(topo) {
			if addr != "" && !seen[addr] {
				seen[addr] = true
				out = append(out, addr)
			}
		}
	}
	return out
}

func (p *Proxy) refreshLeaders(ctx context.Context, topo *cluster.Topology) {
	if topo == nil {
		return
	}
	probes := status.Prober{Client: p.httpClient}.Probe(ctx, cluster.NodeAddresses(topo))
	next := make(map[string]string, len(topo.Shards))
	for _, view := range status.BuildShardViews(topo, probes) {
		if view.LeaderID != "" {
			next[view.ID] = view.LeaderID
		}
	}

	p.mu.Lock()
	p.preferred = next
	p.mu.Unlock()
}

func (p *Proxy) FirstHop(shardID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.preferred[shardID]
}

func (p *Proxy) remember(shardID, nodeID string) {
	if shardID == "" || nodeID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.preferred[shardID] = nodeID
}

func (p *Proxy) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/kv", p.HandleKV)

	mux.HandleFunc("/cluster/topology", routing.ReadOnly(p.HandleTopology))
	mux.HandleFunc("/cluster/locate", p.HandleLocate)
	mux.HandleFunc("/cluster/status", p.HandleClusterStatus)

	mux.HandleFunc("/health", p.HandleHealth)
	mux.HandleFunc("/status", p.HandleStatus)
}

func (p *Proxy) setTopologyHeader(w http.ResponseWriter) {
	w.Header().Set(routing.TopologyVersionHeader, fmt.Sprint(p.peer.Version()))
}

func (p *Proxy) HandleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, routing.MaxBodyBytes))
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}
	var cmd redis.Command
	if err := json.Unmarshal(body, &cmd); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse command: %v", err), http.StatusBadRequest)
		return
	}
	if cmd.Cmd == "" {
		http.Error(w, "missing command", http.StatusBadRequest)
		return
	}

	topo := p.topo.Get()
	resp := routing.KVResponse{
		NodeID:  p.id,
		Command: cmd.Cmd,
		Slot:    -1,
	}
	if topo == nil {
		p.writeKV(w, http.StatusServiceUnavailable,
			resp.WithError("no topology yet; no seed node has answered"))
		return
	}
	resp.TopologyVersion = topo.Version
	dec, err := routing.Route(topo, &cmd, routing.FirstShard)
	resp.Slot = dec.Slot
	resp.Shard = dec.ShardID
	if err != nil {
		p.refuseKV(w, resp, err)
		return
	}

	if !dec.Keyless && r.URL.Query().Get("redirect") != "" {
		resp.Status = "moved"
		resp.Moved = &routing.MovedInfo{Slot: dec.Slot, Shard: dec.ShardID, Nodes: topo.Shards[dec.ShardID].Members}
		p.writeKV(w, http.StatusTemporaryRedirect, resp)
		return
	}

	result, servedBy, err := p.peer.PostToShardMember(
		r.Context(), dec.ShardID, body, p.FirstHop(dec.ShardID), routing.KVURL)
	resp.ServedBy = servedBy
	if err != nil {
		p.writeKV(w, http.StatusBadGateway, resp.WithError(err.Error()))
		return
	}
	p.remember(dec.ShardID, servedBy)

	resp.Status = "success"
	resp.Result = result
	p.writeKV(w, http.StatusOK, resp)
}

func (p *Proxy) refuseKV(w http.ResponseWriter, resp routing.KVResponse, err error) {
	code := http.StatusServiceUnavailable
	var re *routing.Error
	if errors.As(err, &re) {
		code = re.Status4xx()
		if re.RetryAfter {
			w.Header().Set("Retry-After", "1")
		}
	}
	p.writeKV(w, code, resp.WithError(err.Error()))
}

func (p *Proxy) writeKV(w http.ResponseWriter, code int, resp routing.KVResponse) {
	p.setTopologyHeader(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[%s] failed to write /kv response: %v", p.id, err)
	}
}

func (p *Proxy) HandleTopology(w http.ResponseWriter, r *http.Request) {
	topo := p.topo.Get()
	if topo == nil {
		http.Error(w, "no topology yet; no seed node has answered", http.StatusServiceUnavailable)
		return
	}
	p.setTopologyHeader(w)
	if r.URL.Query().Get("summary") != "" {
		routing.WriteJSON(w, http.StatusOK, map[string]any{
			"version":         topo.Version,
			"slot_count":      topo.SlotCount,
			"shards":          topo.Shards,
			"slots_per_shard": topo.SlotCounts(),
			"migrations":      topo.Migrations,
		})
		return
	}
	routing.WriteJSON(w, http.StatusOK, topo)
}

func (p *Proxy) HandleLocate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing 'key' query parameter", http.StatusBadRequest)
		return
	}
	topo := p.topo.Get()
	if topo == nil {
		http.Error(w, "no topology yet; no seed node has answered", http.StatusServiceUnavailable)
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
		payload["first_hop"] = p.FirstHop(shard.ID)
	}
	if mig := topo.MigrationFor(slot); mig != nil {
		payload["migrating"] = mig
	}
	p.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, payload)
}

func (p *Proxy) HandleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topo := p.topo.Get()
	if topo == nil {
		http.Error(w, "no topology yet; no seed node has answered", http.StatusServiceUnavailable)
		return
	}

	p.mu.RLock()
	seed := p.lastSeed
	p.mu.RUnlock()

	probes := status.Prober{Client: p.httpClient}.Probe(r.Context(), cluster.NodeAddresses(topo))
	p.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, status.ClusterStatus{
		ServedBy:    p.id,
		Source:      seed,
		GeneratedAt: time.Now().UnixMilli(),
		Topology:    status.Summarise(topo),
		Shards:      status.BuildShardViews(topo, probes),
		Nodes:       probes,
		Migrations:  topo.Migrations,
	})
}

type SelfStatus struct {
	NodeID          string               `json:"node_id"`
	Role            string               `json:"role"`
	Seeds           []string             `json:"seeds"`
	Source          string               `json:"source,omitempty"`
	TopologyVersion int64                `json:"topology_version"`
	SlotCount       int                  `json:"slot_count"`
	Shards          []status.ShardStatus `json:"shards"`
	FirstHop        map[string]string    `json:"first_hop,omitempty"`
}

func (p *Proxy) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p.mu.RLock()
	seed := p.lastSeed
	hops := make(map[string]string, len(p.preferred))
	for shardID, nodeID := range p.preferred {
		hops[shardID] = nodeID
	}
	p.mu.RUnlock()

	// Shards is empty rather than nil so the dashboard, which polls /status on
	// every address it knows, can iterate it without a special case for a proxy.
	self := SelfStatus{
		NodeID:   p.id,
		Role:     "proxy",
		Seeds:    p.seeds,
		Source:   seed,
		Shards:   []status.ShardStatus{},
		FirstHop: hops,
	}
	if topo := p.topo.Get(); topo != nil {
		self.TopologyVersion = topo.Version
		self.SlotCount = topo.SlotCount
	}
	p.setTopologyHeader(w)
	routing.WriteJSON(w, http.StatusOK, self)
}

func (p *Proxy) HandleHealth(w http.ResponseWriter, r *http.Request) {
	payload := map[string]any{
		"status":  "healthy",
		"role":    "proxy",
		"node_id": p.id,
		"seeds":   p.seeds,
		"routing": false,
	}
	if topo := p.topo.Get(); topo != nil {
		payload["routing"] = true
		payload["topology_version"] = topo.Version
		payload["shards"] = topo.ShardIDs()
	}
	routing.WriteJSON(w, http.StatusOK, payload)
}
