package shard

import (
	"fmt"
	"testing"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/raft"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

func demoShards(urls map[string]string) []*cluster.Shard {
	return []*cluster.Shard{
		{ID: "shard-0", Members: map[string]string{
			"node1": urls["node1"], "node2": urls["node2"], "node3": urls["node3"],
		}},
		{ID: "shard-1", Members: map[string]string{
			"node4": urls["node4"], "node5": urls["node5"], "node6": urls["node6"],
		}},
	}
}

var demoNodes = []string{"node1", "node2", "node3", "node4", "node5", "node6"}

func (tc *testCluster) roles(shardID string) map[raft.NodeRole][]string {
	tc.t.Helper()
	out := map[raft.NodeRole][]string{}
	for _, node := range tc.nodes {
		g, ok := node.manager.group(shardID)
		if !ok {
			continue
		}
		role := g.RaftNode().GetRole()
		out[role] = append(out[role], node.id)
	}
	return out
}

func (tc *testCluster) waitForQuorum(shardID string, replicas int) map[raft.NodeRole][]string {
	tc.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last map[raft.NodeRole][]string
	for time.Now().Before(deadline) {
		last = tc.roles(shardID)
		if len(last[raft.LeaderRole]) == 1 && len(last[raft.FollowerRole]) == replicas-1 {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	tc.t.Fatalf("shard %s never settled on one leader and %d followers: %v", shardID, replicas-1, last)
	return nil
}

func TestTwoShardsOfThreeReplicasElectOneLeaderEach(t *testing.T) {
	tc := newTestCluster(t, demoNodes, demoShards)

	leaders := map[string]string{}
	for _, shardID := range []string{"shard-0", "shard-1"} {
		roles := tc.waitForQuorum(shardID, 3)
		leaders[shardID] = roles[raft.LeaderRole][0]
		t.Logf("%s: leader=%s followers=%v", shardID, roles[raft.LeaderRole][0], roles[raft.FollowerRole])
	}

	if leaders["shard-0"] == leaders["shard-1"] {
		t.Fatalf("both shards report the same leader %q, which cannot be true for disjoint membership",
			leaders["shard-0"])
	}
	for shardID, leader := range leaders {
		if _, member := tc.node(leader).manager.Topology().Shards[shardID].Members[leader]; !member {
			t.Fatalf("%s elected %s, which is not one of its members", shardID, leader)
		}
	}
}

func TestAWriteSentToAFollowerReachesItsLeader(t *testing.T) {
	tc := newTestCluster(t, demoNodes, demoShards)

	for _, shardID := range []string{"shard-0", "shard-1"} {
		roles := tc.waitForQuorum(shardID, 3)
		leader := roles[raft.LeaderRole][0]
		follower := roles[raft.FollowerRole][0]

		key, value := "forwarded:"+shardID, "via-"+follower
		got, err := tc.shardLocal(follower, shardID, setCmd(key, value))
		if err != nil {
			t.Fatalf("SET on follower %s/%s: %v", follower, shardID, err)
		}
		if got != "OK" {
			t.Fatalf("SET on follower %s/%s returned %v, want OK", follower, shardID, got)
		}
		t.Logf("%s: write accepted by follower %s and committed by leader %s", shardID, follower, leader)

		for nodeID := range tc.node(leader).manager.Topology().Shards[shardID].Members {
			tc.waitForLocalCopy(nodeID, shardID, key, value)
		}
	}
}

func TestWritesReplicateToEveryFollower(t *testing.T) {
	tc := newTestCluster(t, demoNodes, demoShards)
	for _, shardID := range []string{"shard-0", "shard-1"} {
		tc.waitForQuorum(shardID, 3)
	}

	written := map[string]string{}
	perShard := map[string]int{}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("session:%d", i)
		value := fmt.Sprintf("token-%d", i)
		resp := tc.kv("node1", setCmd(key, value))
		written[key] = value
		perShard[resp.Shard]++
	}
	if len(perShard) != 2 {
		t.Fatalf("writes only reached %v; both shards should have received keys", perShard)
	}
	t.Logf("keys per shard: %v", perShard)

	for _, entry := range demoNodes {
		for key, want := range written {
			if got := tc.kv(entry, getCmd(key)).Result; got != want {
				t.Fatalf("GET %s via %s = %v, want %v", key, entry, got, want)
			}
		}
	}

	topo := tc.node("node1").manager.Topology()
	for key, want := range written {
		slot := cluster.HashSlot(key, topo.SlotCount)
		shardID := topo.Owner(slot)
		for nodeID := range topo.Shards[shardID].Members {
			tc.waitForLocalCopy(nodeID, shardID, key, want)
		}
	}
}

func (tc *testCluster) waitForLocalCopy(nodeID, shardID, key, want string) {
	tc.t.Helper()
	g, ok := tc.node(nodeID).manager.group(shardID)
	if !ok {
		tc.t.Fatalf("%s does not host %s", nodeID, shardID)
	}

	deadline := time.Now().Add(5 * time.Second)
	var last any
	for time.Now().Before(deadline) {
		got, err := g.Store().EvalAndResponse(&redis.Command{
			Cmd: "GET", Args: map[string]string{"key": key},
		})
		if err == nil && got == want {
			return
		}
		last = got
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("replica %s/%s never applied %s: last read %v, want %v", nodeID, shardID, key, last, want)
}

func TestRebalanceAcrossReplicatedShards(t *testing.T) {
	if testing.Short() {
		t.Skip("six Raft groups take a few seconds to settle")
	}
	tc := newTestCluster(t, demoNodes, demoShards)
	for _, shardID := range []string{"shard-0", "shard-1"} {
		tc.waitForQuorum(shardID, 3)
	}

	written := map[string]string{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("order:%d", i)
		value := fmt.Sprintf("amount-%d", i)
		tc.kv("node2", setCmd(key, value))
		written[key] = value
	}

	next := demoShards(map[string]string{
		"node1": tc.node("node1").url, "node2": tc.node("node2").url, "node3": tc.node("node3").url,
		"node4": tc.node("node4").url, "node5": tc.node("node5").url, "node6": tc.node("node6").url,
	})
	next = append(next, &cluster.Shard{ID: "shard-2", Members: map[string]string{
		"node1": tc.node("node1").url,
		"node4": tc.node("node4").url,
		"node6": tc.node("node6").url,
	}})

	report, status := tc.rebalance("node1", rebalanceRequestBody{Shards: next})
	if status != 200 || report.Failures != 0 {
		t.Fatalf("rebalance failed (HTTP %d): %+v", status, report)
	}
	t.Logf("moved %d slots and %d keys in %s", report.MigratedSlots, report.KeysMoved, report.Duration)

	after := tc.node("node1").manager.Topology()
	if after.SlotCounts()["shard-2"] == 0 {
		t.Fatalf("the new group owns no slots: %v", after.SlotCounts())
	}
	tc.waitForQuorum("shard-2", 3)

	for _, entry := range demoNodes {
		for key, want := range written {
			if got := tc.kv(entry, getCmd(key)).Result; got != want {
				t.Fatalf("after rebalance, GET %s via %s = %v, want %v", key, entry, got, want)
			}
		}
	}
	t.Logf("slots per shard after rebalance: %v", after.SlotCounts())
}
