package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/proxy"
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

const deadSeed = "http://127.0.0.1:1"

type testProxy struct {
	t     *testing.T
	proxy *proxy.Proxy
	url   string
}

func (tc *testCluster) startProxy(cfg proxy.Config) *testProxy {
	tc.t.Helper()
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 5 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}

	proxy, err := proxy.New(cfg)
	if err != nil {
		tc.t.Fatalf("start proxy: %v", err)
	}
	tc.t.Cleanup(proxy.Stop)

	mux := http.NewServeMux()
	proxy.Routes(mux)
	srv := httptest.NewServer(mux)
	tc.t.Cleanup(srv.Close)

	return &testProxy{t: tc.t, proxy: proxy, url: srv.URL}
}

// seedURLs turns node IDs into the seed list an operator would configure.
func (tc *testCluster) seedURLs(nodeIDs ...string) []string {
	tc.t.Helper()
	out := make([]string, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		out = append(out, tc.node(id).url)
	}
	return out
}

func (tp *testProxy) kvRaw(cmd redis.Command) (routing.KVResponse, int) {
	tp.t.Helper()
	body, err := json.Marshal(cmd)
	if err != nil {
		tp.t.Fatal(err)
	}
	httpResp, err := http.Post(tp.url+"/kv", "application/json", bytes.NewReader(body))
	if err != nil {
		tp.t.Fatalf("POST proxy /kv: %v", err)
	}
	defer httpResp.Body.Close()

	raw, _ := io.ReadAll(httpResp.Body)
	var decoded routing.KVResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		tp.t.Fatalf("decode proxy /kv response (%s): %v", raw, err)
	}
	return decoded, httpResp.StatusCode
}

func (tp *testProxy) kv(cmd redis.Command) routing.KVResponse {
	tp.t.Helper()
	resp, status := tp.kvRaw(cmd)
	if status != http.StatusOK {
		tp.t.Fatalf("%s %v via proxy: HTTP %d: %s", cmd.Cmd, cmd.Args, status, resp.Error)
	}
	return resp
}

func twoShardCluster(t *testing.T) *testCluster {
	t.Helper()
	return newTestCluster(t, []string{"node1", "node2"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node2", urls["node2"]),
		}
	})
}

func TestProxyRoutesEachKeyToTheOwningShard(t *testing.T) {
	tc := twoShardCluster(t)
	tp := tc.startProxy(proxy.Config{ID: "proxy-1", Seeds: tc.seedURLs("node1")})

	topo := tc.node("node1").manager.Topology()
	if got := tp.proxy.Topology(); got == nil || got.Version != topo.Version {
		t.Fatalf("proxy did not read the cluster topology: %v", got)
	}

	written := map[string]string{}
	perShard := map[string]int{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("item:%d", i)
		value := fmt.Sprintf("v%d", i)
		resp := tp.kv(setCmd(key, value))
		written[key] = value

		wantSlot := cluster.HashSlot(key, topo.SlotCount)
		if resp.Slot != wantSlot {
			t.Fatalf("%s routed to slot %d, want %d", key, resp.Slot, wantSlot)
		}
		if want := topo.Owner(wantSlot); resp.Shard != want {
			t.Fatalf("%s routed to shard %s, but slot %d is owned by %s", key, resp.Shard, wantSlot, want)
		}
		if resp.NodeID != "proxy-1" {
			t.Fatalf("response came from %q, want the proxy", resp.NodeID)
		}
		if resp.ServedBy == "" || resp.ServedBy == "proxy-1" {
			t.Fatalf("%s reports served_by %q; the proxy hosts no shard and must name a node", key, resp.ServedBy)
		}
		perShard[resp.Shard]++
	}
	if len(perShard) < 2 {
		t.Fatalf("all keys landed on one shard: %v", perShard)
	}
	t.Logf("keys per shard through the proxy: %v", perShard)
	for key, want := range written {
		if got := tp.kv(getCmd(key)).Result; got != want {
			t.Fatalf("GET %s via proxy = %v, want %v", key, got, want)
		}

		owner := topo.Owner(cluster.HashSlot(key, topo.SlotCount))
		nodeID := "node1"
		if owner == "shard-b" {
			nodeID = "node2"
		}
		got, err := tc.shardLocal(nodeID, owner, getCmd(key))
		if err != nil || got != want {
			t.Fatalf("%s should sit in %s on %s, got (%v, %v)", key, owner, nodeID, got, err)
		}
	}
}

func TestProxyRoutesKeylessCommandsDeterministically(t *testing.T) {
	tc := twoShardCluster(t)
	first := tc.startProxy(proxy.Config{ID: "proxy-1", Seeds: tc.seedURLs("node1")})
	second := tc.startProxy(proxy.Config{ID: "proxy-2", Seeds: tc.seedURLs("node2")})

	cmd := redis.Command{Cmd: "PING"}
	a, statusA := first.kvRaw(cmd)
	b, statusB := second.kvRaw(cmd)

	if statusA != http.StatusOK || statusB != http.StatusOK {
		t.Fatalf("keyless command failed: proxy-1 HTTP %d (%s), proxy-2 HTTP %d (%s)",
			statusA, a.Error, statusB, b.Error)
	}
	if a.Slot != -1 || b.Slot != -1 {
		t.Fatalf("a keyless command should have no slot, got %d and %d", a.Slot, b.Slot)
	}
	if a.Shard != b.Shard {
		t.Fatalf("two proxies sent the same keyless command to different shards: %s vs %s", a.Shard, b.Shard)
	}
	if want := tc.node("node1").manager.Topology().ShardIDs()[0]; a.Shard != want {
		t.Fatalf("keyless command went to shard %s, want the lowest shard ID %s", a.Shard, want)
	}
}

func TestStaleProxyStillLandsWritesInTheRightShard(t *testing.T) {
	tc := twoShardCluster(t)
	real := tc.node("node1").manager.Topology()
	stale, err := cluster.NewTopology(
		[]*cluster.Shard{shardOn("shard-a", "node1", tc.node("node1").url)},
		real.SlotCount, real.VNodes, real.Epsilon)
	if err != nil {
		t.Fatalf("build stale topology: %v", err)
	}
	path := filepath.Join(t.TempDir(), "stale-topology.json")
	if err := stale.Save(path); err != nil {
		t.Fatalf("save stale topology: %v", err)
	}

	tp := tc.startProxy(proxy.Config{
		ID:           "stale-proxy",
		Seeds:        []string{deadSeed},
		TopologyPath: path,
		// Long enough that no background refresh can rescue the proxy before
		// the assertions below run.
		RefreshInterval: time.Hour,
		RequestTimeout:  2 * time.Second,
	})
	if got := tp.proxy.Topology(); got == nil || len(got.Shards) != 1 {
		t.Fatalf("proxy should have come up on the stale cached topology, got %v", got)
	}
	var key string
	for i := 0; i < 200; i++ {
		candidate := fmt.Sprintf("stale:%d", i)
		if real.Owner(cluster.HashSlot(candidate, real.SlotCount)) == "shard-b" {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Skip("no key in this topology is owned by shard-b")
	}

	resp := tp.kv(setCmd(key, "written-while-stale"))
	if resp.Shard != "shard-a" {
		t.Fatalf("the stale proxy was expected to aim at shard-a, got %s", resp.Shard)
	}

	// The node it forwarded to knew better, so the value must be in shard-b.
	got, err := tc.shardLocal("node2", "shard-b", getCmd(key))
	if err != nil || got != "written-while-stale" {
		t.Fatalf("%s should have landed in shard-b despite the stale proxy, got (%v, %v)", key, got, err)
	}
	if got, err := tc.shardLocal("node1", "shard-a", getCmd(key)); err == nil && got != nil && got != "" {
		t.Fatalf("shard-a should not hold %s, but returned %v", key, got)
	}
}

// The cached topology is a starting point, not the truth: a reachable seed
// replaces it during startup.
func TestProxyPrefersASeedOverItsCachedTopology(t *testing.T) {
	tc := twoShardCluster(t)
	real := tc.node("node1").manager.Topology()

	stale, err := cluster.NewTopology(
		[]*cluster.Shard{shardOn("shard-a", "node1", tc.node("node1").url)},
		real.SlotCount, real.VNodes, real.Epsilon)
	if err != nil {
		t.Fatalf("build stale topology: %v", err)
	}
	path := filepath.Join(t.TempDir(), "stale-topology.json")
	if err := stale.Save(path); err != nil {
		t.Fatalf("save stale topology: %v", err)
	}

	tp := tc.startProxy(proxy.Config{ID: "proxy-1", Seeds: tc.seedURLs("node1"), TopologyPath: path})
	got := tp.proxy.Topology()
	if got == nil || got.Version != real.Version || len(got.Shards) != len(real.Shards) {
		t.Fatalf("proxy kept its cached topology instead of reading the cluster's: %v", got)
	}
}

// Seeds are entry points, not a dependency list: the first one being down must
// not stop the proxy.
func TestProxyStartsFromASurvivingSeed(t *testing.T) {
	tc := twoShardCluster(t)
	tp := tc.startProxy(proxy.Config{
		ID:    "proxy-1",
		Seeds: append([]string{deadSeed}, tc.seedURLs("node2")...),
	})

	if tp.proxy.Topology() == nil {
		t.Fatal("proxy found no topology although one seed was up")
	}
	if got := tp.kv(setCmd("through-second-seed", "ok")).Result; got != "OK" {
		t.Fatalf("SET via the proxy returned %v, want OK", got)
	}
	if got := tp.kv(getCmd("through-second-seed")).Result; got != "ok" {
		t.Fatalf("GET returned %v, want ok", got)
	}
}

func TestProxyWithoutATopologyRefusesRatherThanFails(t *testing.T) {
	proxy, err := proxy.New(proxy.Config{
		ID:              "orphan",
		Seeds:           []string{deadSeed},
		RefreshInterval: time.Hour,
		RequestTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("a proxy that cannot reach its seeds should still start: %v", err)
	}
	t.Cleanup(proxy.Stop)

	if proxy.Topology() != nil {
		t.Fatal("proxy reported a topology it never read")
	}

	mux := http.NewServeMux()
	proxy.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body, _ := json.Marshal(setCmd("k", "v"))
	resp, err := http.Post(srv.URL+"/kv", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /kv: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/kv returned HTTP %d, want 503 while the proxy has no topology", resp.StatusCode)
	}

	health, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("/health returned HTTP %d; the proxy process is up even when the cluster is not", health.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(health.Body).Decode(&payload); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if payload["routing"] != false {
		t.Fatalf("/health should report routing=false without a topology, got %v", payload["routing"])
	}
}

// The proxy must never be a way around the control plane.
func TestProxyRejectsTopologyWrites(t *testing.T) {
	tc := twoShardCluster(t)
	tp := tc.startProxy(proxy.Config{ID: "proxy-1", Seeds: tc.seedURLs("node1")})

	before := tp.proxy.Topology().Version
	body, _ := json.Marshal(tp.proxy.Topology())
	resp, err := http.Post(tp.url+"/cluster/topology", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /cluster/topology: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /cluster/topology returned HTTP %d, want 405", resp.StatusCode)
	}
	if after := tp.proxy.Topology().Version; after != before {
		t.Fatalf("topology changed from v%d to v%d through the proxy", before, after)
	}

	for _, path := range []string{"/cluster/rebalance", "/cluster/shards", "/raft/append", "/raft/vote"} {
		resp, err := http.Post(tp.url+path, "application/json", bytes.NewReader(nil))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("the proxy answered %s with HTTP %d; it must not expose the control plane or Raft RPC",
				path, resp.StatusCode)
		}
	}
}

func TestProxyLearnsEachShardsLeaderAsItsFirstHop(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2", "node3"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			{ID: "shard-a", Members: map[string]string{
				"node1": urls["node1"], "node2": urls["node2"], "node3": urls["node3"],
			}},
		}
	})
	tp := tc.startProxy(proxy.Config{ID: "proxy-1", Seeds: tc.seedURLs("node1")})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tp.proxy.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	hop := tp.proxy.FirstHop("shard-a")
	if hop == "" {
		t.Fatal("proxy learned no first hop for shard-a")
	}
	leader := tc.node(hop).manager.mustGroup(t, "shard-a").RaftNode().GetLeaderID()
	if hop != leader {
		t.Fatalf("proxy aims at %s but the shard's leader is %s", hop, leader)
	}

	// Whichever member it aims at, the command still has to work.
	if got := tp.kv(setCmd("replicated", "value")).Result; got != "OK" {
		t.Fatalf("SET via proxy returned %v, want OK", got)
	}
	if got := tp.kv(getCmd("replicated")).Result; got != "value" {
		t.Fatalf("GET via proxy returned %v, want value", got)
	}
}

func (m *Manager) mustGroup(t *testing.T, shardID string) *redis.RaftRedisServer {
	t.Helper()
	g, ok := m.group(shardID)
	if !ok {
		t.Fatalf("%s does not host %s", m.nodeID, shardID)
	}
	return g
}
