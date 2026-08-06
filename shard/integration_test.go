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
	"github.com/namnv2496/go-redis-raft/routing"
	"github.com/namnv2496/go-redis-raft/shard/redis"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

const testSlotCount = 256

type testNode struct {
	id      string
	url     string
	manager *Manager
}

type testCluster struct {
	t     *testing.T
	nodes map[string]*testNode
}

func newTestCluster(t *testing.T, nodeIDs []string, build func(urls map[string]string) []*cluster.Shard) *testCluster {
	t.Helper()

	muxes := make(map[string]*http.ServeMux, len(nodeIDs))
	urls := make(map[string]string, len(nodeIDs))
	for _, id := range nodeIDs {
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		muxes[id] = mux
		urls[id] = srv.URL
	}

	topo, err := cluster.NewTopology(build(urls), testSlotCount, cluster.DefaultVNodes, cluster.DefaultEpsilon)
	if err != nil {
		t.Fatalf("build topology: %v", err)
	}

	tc := &testCluster{t: t, nodes: make(map[string]*testNode, len(nodeIDs))}
	for _, id := range nodeIDs {
		dir := filepath.Join(t.TempDir(), id)
		manager, err := New(Config{
			NodeID:           id,
			Advertise:        urls[id],
			DataDir:          dir,
			EvictStrategy:    data_structure.EvictFirst,
			Bootstrap:        topo.Clone(),
			TopologyPath:     filepath.Join(dir, "topology.json"),
			ElectionTimeout:  150 * time.Millisecond,
			HeartbeatTimeout: 30 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("start node %s: %v", id, err)
		}
		t.Cleanup(manager.Stop)
		manager.Routes(muxes[id])
		tc.nodes[id] = &testNode{id: id, url: urls[id], manager: manager}
	}

	tc.waitForLeaders()
	return tc
}

func (tc *testCluster) waitForLeaders() {
	tc.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, node := range tc.nodes {
		for _, shardID := range node.manager.LocalShards() {
			if err := node.manager.WaitShardLeader(ctx, shardID, 10*time.Second); err != nil {
				tc.t.Fatalf("shard %s on %s: %v", shardID, node.id, err)
			}
		}
	}
}

func (tc *testCluster) node(id string) *testNode {
	tc.t.Helper()
	n, ok := tc.nodes[id]
	if !ok {
		tc.t.Fatalf("no node %q in this cluster", id)
	}
	return n
}

func (tc *testCluster) kv(nodeID string, cmd redis.Command) routing.KVResponse {
	tc.t.Helper()
	resp, status := tc.kvRaw(nodeID, cmd, "")
	if status != http.StatusOK {
		tc.t.Fatalf("%s %v on %s: HTTP %d: %s", cmd.Cmd, cmd.Args, nodeID, status, resp.Error)
	}
	return resp
}

func (tc *testCluster) kvRaw(nodeID string, cmd redis.Command, query string) (routing.KVResponse, int) {
	tc.t.Helper()
	body, err := json.Marshal(cmd)
	if err != nil {
		tc.t.Fatal(err)
	}
	url := tc.node(nodeID).url + "/kv"
	if query != "" {
		url += "?" + query
	}
	httpResp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		tc.t.Fatalf("POST /kv: %v", err)
	}
	defer httpResp.Body.Close()

	var decoded routing.KVResponse
	raw, _ := io.ReadAll(httpResp.Body)
	if err := json.Unmarshal(raw, &decoded); err != nil {
		tc.t.Fatalf("decode /kv response (%s): %v", raw, err)
	}
	return decoded, httpResp.StatusCode
}

func (tc *testCluster) shardLocal(nodeID, shardID string, cmd redis.Command) (any, error) {
	tc.t.Helper()
	body, err := json.Marshal(cmd)
	if err != nil {
		tc.t.Fatal(err)
	}
	httpResp, err := http.Post(
		tc.node(nodeID).url+"/shards/"+shardID+"/raft/command", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var env routing.CommandEnvelope
	if err := json.NewDecoder(httpResp.Body).Decode(&env); err != nil {
		return nil, err
	}
	if env.Error != "" {
		return nil, fmt.Errorf("%s", env.Error)
	}
	return env.Result, nil
}

func (tc *testCluster) rebalance(nodeID string, req rebalanceRequestBody) (*RebalanceReport, int) {
	tc.t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		tc.t.Fatal(err)
	}
	httpResp, err := http.Post(tc.node(nodeID).url+"/cluster/rebalance", "application/json", bytes.NewReader(body))
	if err != nil {
		tc.t.Fatalf("POST /cluster/rebalance: %v", err)
	}
	defer httpResp.Body.Close()

	raw, _ := io.ReadAll(httpResp.Body)
	var report RebalanceReport
	if err := json.Unmarshal(raw, &report); err != nil {
		tc.t.Fatalf("decode rebalance report (%s): %v", raw, err)
	}
	return &report, httpResp.StatusCode
}

func shardOn(id, nodeID, url string) *cluster.Shard {
	return &cluster.Shard{ID: id, Members: map[string]string{nodeID: url}}
}

func setCmd(key, value string) redis.Command {
	return redis.Command{Cmd: "SET", Args: map[string]string{"key": key, "value": value}}
}

func getCmd(key string) redis.Command {
	return redis.Command{Cmd: "GET", Args: map[string]string{"key": key}}
}

func TestRoutedAPISendsKeysToTheOwningShard(t *testing.T) {
	tc := newTestCluster(t, []string{"node1"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node1", urls["node1"]),
		}
	})
	topo := tc.node("node1").manager.Topology()

	perShard := map[string]int{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("user:%d", i)
		resp := tc.kv("node1", setCmd(key, fmt.Sprintf("v%d", i)))

		wantSlot := cluster.HashSlot(key, topo.SlotCount)
		if resp.Slot != wantSlot {
			t.Fatalf("%s routed to slot %d, want %d", key, resp.Slot, wantSlot)
		}
		if resp.Shard != topo.Owner(wantSlot) {
			t.Fatalf("%s routed to shard %s, but slot %d is owned by %s",
				key, resp.Shard, wantSlot, topo.Owner(wantSlot))
		}
		perShard[resp.Shard]++
	}
	if len(perShard) < 2 {
		t.Fatalf("all keys landed on one shard: %v", perShard)
	}
	t.Logf("keys per shard: %v", perShard)

	key := "user:7"
	slot := cluster.HashSlot(key, topo.SlotCount)
	owner, other := topo.Owner(slot), "shard-a"
	if owner == "shard-a" {
		other = "shard-b"
	}
	got, err := tc.shardLocal("node1", owner, getCmd(key))
	if err != nil || got != "v7" {
		t.Fatalf("owning shard %s returned (%v, %v), want v7", owner, got, err)
	}
	if got, err := tc.shardLocal("node1", other, getCmd(key)); err == nil && got != nil && got != "" {
		t.Fatalf("shard %s should not hold %s, but returned %v", other, key, got)
	}
}

func TestRoutedAPIProxiesToTheNodeThatOwnsTheSlot(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node2", urls["node2"]),
		}
	})

	written := map[string]string{}
	proxied := 0
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("item:%d", i)
		value := fmt.Sprintf("v%d", i)
		resp := tc.kv("node1", setCmd(key, value))
		written[key] = value
		if resp.ServedBy != "node1" {
			proxied++
		}
	}
	if proxied == 0 {
		t.Fatal("no write was proxied to node2; the two shards were not exercised")
	}
	t.Logf("%d/%d writes were proxied to the other node", proxied, len(written))

	for _, entry := range []string{"node1", "node2"} {
		for key, want := range written {
			if got := tc.kv(entry, getCmd(key)).Result; got != want {
				t.Fatalf("GET %s via %s = %v, want %v", key, entry, got, want)
			}
		}
	}
}

func TestRedirectModeReturnsTheOwningShard(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node2", urls["node2"]),
		}
	})

	resp, status := tc.kvRaw("node1", getCmd("anything"), "redirect=1")
	if status != http.StatusTemporaryRedirect {
		t.Fatalf("redirect mode returned HTTP %d, want %d", status, http.StatusTemporaryRedirect)
	}
	if resp.Moved == nil {
		t.Fatal("no routing information in the redirect")
	}
	topo := tc.node("node1").manager.Topology()
	if resp.Moved.Shard != topo.Owner(resp.Slot) {
		t.Fatalf("redirected to shard %s, want %s", resp.Moved.Shard, topo.Owner(resp.Slot))
	}
	if len(resp.Moved.Nodes) == 0 {
		t.Fatal("redirect named no node to talk to")
	}
}

func TestRebalanceMovesDataToANewShard(t *testing.T) {
	tc := newTestCluster(t, []string{"node1", "node2"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node2", urls["node2"]),
		}
	})

	written := map[string]string{}
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("value-%d", i)
		tc.kv("node1", setCmd(key, value))
		written[key] = value
	}

	before := tc.node("node1").manager.Topology()
	newShards := []*cluster.Shard{
		shardOn("shard-a", "node1", tc.node("node1").url),
		shardOn("shard-b", "node2", tc.node("node2").url),
		shardOn("shard-c", "node2", tc.node("node2").url),
	}

	dry, status := tc.rebalance("node1", rebalanceRequestBody{Shards: newShards, DryRun: true})
	if status != http.StatusOK {
		t.Fatalf("dry run returned HTTP %d", status)
	}
	if dry.PlannedMoves == 0 {
		t.Fatal("dry run planned no moves for a new shard")
	}
	if got := tc.node("node1").manager.Topology().Version; got != before.Version {
		t.Fatalf("dry run changed the topology: v%d -> v%d", before.Version, got)
	}

	report, status := tc.rebalance("node1", rebalanceRequestBody{Shards: newShards})
	if status != http.StatusOK {
		t.Fatalf("rebalance returned HTTP %d: %+v", status, report)
	}
	if report.Failures != 0 {
		t.Fatalf("%d slot migrations failed: %+v", report.Failures, report.Batches)
	}
	if report.MigratedSlots != dry.PlannedMoves {
		t.Fatalf("migrated %d slots, planned %d", report.MigratedSlots, dry.PlannedMoves)
	}
	if report.KeysMoved == 0 {
		t.Fatal("the new shard received no keys at all")
	}
	t.Logf("moved %d slots and %d keys in %s", report.MigratedSlots, report.KeysMoved, report.Duration)

	after := tc.node("node1").manager.Topology()
	if after.Version <= before.Version {
		t.Fatalf("topology version did not advance: %d -> %d", before.Version, after.Version)
	}
	if counts := after.SlotCounts(); counts["shard-c"] == 0 {
		t.Fatalf("new shard owns no slots: %v", counts)
	} else {
		t.Logf("slots per shard after rebalance: %v", counts)
	}
	if len(after.Migrations) != 0 {
		t.Fatalf("migrations left in flight: %+v", after.Migrations)
	}

	if v := tc.node("node2").manager.Topology().Version; v != after.Version {
		t.Fatalf("node2 is on topology v%d, node1 on v%d", v, after.Version)
	}

	for _, entry := range []string{"node1", "node2"} {
		for key, want := range written {
			if got := tc.kv(entry, getCmd(key)).Result; got != want {
				t.Fatalf("after rebalance, GET %s via %s = %v, want %v", key, entry, got, want)
			}
		}
	}

	moved := 0
	for key := range written {
		slot := cluster.HashSlot(key, after.SlotCount)
		if after.Owner(slot) != "shard-c" {
			continue
		}
		moved++
		got, err := tc.shardLocal("node2", "shard-c", getCmd(key))
		if err != nil || got != written[key] {
			t.Fatalf("shard-c does not hold %s: (%v, %v)", key, got, err)
		}
	}
	if moved == 0 {
		t.Fatal("no key ended up on the new shard")
	}
	t.Logf("%d/%d keys now live on shard-c", moved, len(written))
}

func TestRebalanceDrainsARemovedShard(t *testing.T) {
	tc := newTestCluster(t, []string{"node1"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node1", urls["node1"]),
			shardOn("shard-c", "node1", urls["node1"]),
		}
	})

	written := map[string]string{}
	for i := 0; i < 90; i++ {
		key := fmt.Sprintf("row:%d", i)
		value := fmt.Sprintf("v%d", i)
		tc.kv("node1", setCmd(key, value))
		written[key] = value
	}

	manager := tc.node("node1").manager
	remaining, err := cluster.RemoveShard(manager.Topology(), "shard-c")
	if err != nil {
		t.Fatal(err)
	}
	report, status := tc.rebalance("node1", rebalanceRequestBody{Shards: remaining})
	if status != http.StatusOK || report.Failures != 0 {
		t.Fatalf("rebalance failed (HTTP %d): %+v", status, report)
	}

	after := manager.Topology()
	if _, still := after.Shards["shard-c"]; still {
		t.Fatalf("drained shard is still in the topology: %v", after.ShardIDs())
	}
	if hosting := manager.LocalShards(); len(hosting) != 2 {
		t.Fatalf("node still runs %v; the retired group should have been stopped", hosting)
	}
	for key, want := range written {
		if got := tc.kv("node1", getCmd(key)).Result; got != want {
			t.Fatalf("after draining shard-c, GET %s = %v, want %v", key, got, want)
		}
	}
}

func TestRebalanceIsIdempotent(t *testing.T) {
	tc := newTestCluster(t, []string{"node1"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node1", urls["node1"]),
		}
	})
	tc.kv("node1", setCmd("stable", "value"))

	report, status := tc.rebalance("node1", rebalanceRequestBody{})
	if status != http.StatusOK {
		t.Fatalf("rebalance returned HTTP %d: %+v", status, report)
	}
	if report.PlannedMoves != 0 {
		t.Fatalf("a balanced cluster planned %d moves", report.PlannedMoves)
	}
	if got := tc.kv("node1", getCmd("stable")).Result; got != "value" {
		t.Fatalf("GET after no-op rebalance = %v", got)
	}
}

func TestUnroutableCommandStillRuns(t *testing.T) {
	tc := newTestCluster(t, []string{"node1"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{shardOn("shard-a", "node1", urls["node1"])}
	})

	resp := tc.kv("node1", redis.Command{Cmd: "PING"})
	if resp.Result != "PONG" {
		t.Fatalf("PING returned %v, want PONG", resp.Result)
	}
	if resp.Slot != -1 {
		t.Fatalf("a keyless command was assigned slot %d", resp.Slot)
	}
}

func TestLocateReportsWhereAKeyLives(t *testing.T) {
	tc := newTestCluster(t, []string{"node1"}, func(urls map[string]string) []*cluster.Shard {
		return []*cluster.Shard{
			shardOn("shard-a", "node1", urls["node1"]),
			shardOn("shard-b", "node1", urls["node1"]),
		}
	})

	resp, err := http.Get(tc.node("node1").url + "/cluster/locate?key=%7Buser:42%7D:profile")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var located struct {
		Key     string `json:"key"`
		HashTag string `json:"hash_tag"`
		Slot    int    `json:"slot"`
		Shard   string `json:"shard"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&located); err != nil {
		t.Fatal(err)
	}
	if located.HashTag != "user:42" {
		t.Fatalf("hash tag resolved to %q", located.HashTag)
	}
	topo := tc.node("node1").manager.Topology()
	if located.Slot != cluster.HashSlot("{user:42}:profile", topo.SlotCount) {
		t.Fatalf("locate reported slot %d", located.Slot)
	}
	if located.Shard != topo.Owner(located.Slot) {
		t.Fatalf("locate reported shard %s, want %s", located.Shard, topo.Owner(located.Slot))
	}
}
