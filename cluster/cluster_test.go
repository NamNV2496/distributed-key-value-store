package cluster

import (
	"fmt"
	"testing"
)

func shardSet(ids ...string) []*Shard {
	out := make([]*Shard, 0, len(ids))
	for _, id := range ids {
		out = append(out, &Shard{
			ID:      id,
			Members: map[string]string{id + "-node": "http://" + id + ":5000"},
		})
	}
	return out
}

func TestHashTag(t *testing.T) {
	cases := map[string]string{
		"foo":                "foo",
		"{user:42}:profile":  "user:42",
		"{user:42}:sessions": "user:42",
		"prefix{tag}suffix":  "tag",
		"{}empty":            "{}empty",
		"unclosed{tag":       "unclosed{tag",
		"{first}{second}":    "first",
	}
	for key, want := range cases {
		if got := HashTag(key); got != want {
			t.Errorf("HashTag(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestHashSlotIsStableAndInRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		slot := HashSlot(key, DefaultSlotCount)
		if slot < 0 || slot >= DefaultSlotCount {
			t.Fatalf("slot %d for %q out of range", slot, key)
		}
		if again := HashSlot(key, DefaultSlotCount); again != slot {
			t.Fatalf("HashSlot(%q) not stable: %d then %d", key, slot, again)
		}
	}
}

func TestHashSlotGroupsHashTags(t *testing.T) {
	a := HashSlot("{user:42}:profile", DefaultSlotCount)
	b := HashSlot("{user:42}:sessions", DefaultSlotCount)
	if a != b {
		t.Fatalf("hash-tagged keys landed in different slots: %d vs %d", a, b)
	}
}

func TestHashSlotSpreadsKeys(t *testing.T) {
	const slots = 256
	seen := make(map[int]int)
	for i := 0; i < 20000; i++ {
		seen[HashSlot(fmt.Sprintf("key:%d", i), slots)]++
	}
	if len(seen) != slots {
		t.Fatalf("only %d/%d slots were used", len(seen), slots)
	}
}

func TestRingAssignmentIsDeterministic(t *testing.T) {
	a := NewRing([]string{"s1", "s2", "s3"}, DefaultVNodes, DefaultEpsilon).AssignSlots(DefaultSlotCount)

	b := NewRing([]string{"s3", "s1", "s2"}, DefaultVNodes, DefaultEpsilon).AssignSlots(DefaultSlotCount)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("slot %d: %q vs %q", i, a[i], b[i])
		}
	}
}

func TestRingRespectsLoadCap(t *testing.T) {
	for _, n := range []int{2, 3, 4, 5, 8} {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("shard-%d", i)
		}
		ring := NewRing(ids, DefaultVNodes, DefaultEpsilon)
		counts := map[string]int{}
		for _, owner := range ring.AssignSlots(DefaultSlotCount) {
			counts[owner]++
		}
		limit := ring.capacity(DefaultSlotCount)
		for id, c := range counts {
			if c > limit {
				t.Errorf("n=%d shard %s owns %d slots, cap is %d", n, id, c, limit)
			}
		}
		if len(counts) != n {
			t.Errorf("n=%d: only %d shards received slots", n, len(counts))
		}
	}
}

func TestNewTopologyOwnsEverySlot(t *testing.T) {
	topo, err := NewTopology(shardSet("s1", "s2", "s3"), 1024, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	if err := topo.Validate(); err != nil {
		t.Fatal(err)
	}
	for slot := 0; slot < topo.SlotCount; slot++ {
		if topo.Owner(slot) == "" {
			t.Fatalf("slot %d unowned", slot)
		}
	}
	if _, shard := topo.Locate("hello"); shard == nil {
		t.Fatal("Locate returned no shard")
	}
}

func TestAddingShardMovesRoughlyItsFairShare(t *testing.T) {
	cur, err := NewTopology(shardSet("s1", "s2", "s3", "s4"), DefaultSlotCount, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	next, err := AddShard(cur, &Shard{ID: "s5", Members: map[string]string{"n5": "http://n5:5000"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := ComputePlan(cur, next)
	if err != nil {
		t.Fatal(err)
	}

	fairShare := DefaultSlotCount / 5
	if plan.MovedSlots > 2*fairShare {
		t.Fatalf("moved %d slots, expected around %d (a rebalance should not reshuffle the cluster)",
			plan.MovedSlots, fairShare)
	}

	if got := plan.After["s5"]; got < fairShare/2 {
		t.Fatalf("new shard only received %d slots, fair share is %d", got, fairShare)
	}

	toNew := 0
	for _, m := range plan.Moves {
		if m.To == "s5" {
			toNew++
		}
	}
	if toNew != plan.After["s5"] {
		t.Fatalf("s5 owns %d slots but only %d moves target it", plan.After["s5"], toNew)
	}
	t.Logf("4->5 shards: moved %d/%d slots (fair share %d)", plan.MovedSlots, DefaultSlotCount, fairShare)
}

func TestRemovingShardOnlyDrainsThatShard(t *testing.T) {
	cur, err := NewTopology(shardSet("s1", "s2", "s3", "s4"), DefaultSlotCount, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	next, err := RemoveShard(cur, "s3")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := ComputePlan(cur, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, still := plan.After["s3"]; still {
		t.Fatal("removed shard still owns slots")
	}
	drained := plan.Before["s3"]
	if plan.MovedSlots < drained {
		t.Fatalf("only %d moves for %d orphaned slots", plan.MovedSlots, drained)
	}

	if extra := plan.MovedSlots - drained; extra > drained/2 {
		t.Fatalf("removing one shard shuffled %d extra slots on top of the %d it had to", extra, drained)
	}
	t.Logf("4->3 shards: moved %d slots, %d of them orphaned by the removal", plan.MovedSlots, drained)
}

func TestRemoveLastShardIsRejected(t *testing.T) {
	cur, err := NewTopology(shardSet("only"), 64, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RemoveShard(cur, "only"); err == nil {
		t.Fatal("expected removing the last shard to fail")
	}
}

func TestComputePlanRefusesWhileSlotsAreInFlight(t *testing.T) {
	cur, err := NewTopology(shardSet("s1", "s2"), 64, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	cur = cur.WithMigration(Migration{Slot: 7, From: "s1", To: "s2", State: MigrationCopying})
	if _, err := ComputePlan(cur, shardSet("s1", "s2", "s3")); err == nil {
		t.Fatal("expected a plan over a migrating cluster to be refused")
	}
}

func TestWithSlotOwnerClearsMigration(t *testing.T) {
	topo, err := NewTopology(shardSet("s1", "s2"), 64, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	before := topo.Version
	moving := topo.WithMigration(Migration{Slot: 5, From: "s1", To: "s2", State: MigrationCopying})
	if moving.MigrationFor(5) == nil {
		t.Fatal("migration not recorded")
	}
	if topo.MigrationFor(5) != nil {
		t.Fatal("WithMigration mutated the original topology")
	}
	done := moving.WithSlotOwner(5, "s2")
	if done.MigrationFor(5) != nil {
		t.Fatal("migration not cleared after ownership flip")
	}
	if done.Owner(5) != "s2" {
		t.Fatalf("slot 5 owned by %q, want s2", done.Owner(5))
	}
	if done.Version <= before {
		t.Fatalf("version did not advance: %d -> %d", before, done.Version)
	}
}

func TestStoreIgnoresOlderTopology(t *testing.T) {
	first, err := NewTopology(shardSet("s1"), 64, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	store := NewStore(first, "")

	newer := first.Clone()
	newer.Version = 5
	if installed, err := store.Install(newer); err != nil || !installed {
		t.Fatalf("newer topology rejected: installed=%v err=%v", installed, err)
	}

	older := first.Clone()
	older.Version = 2
	if installed, err := store.Install(older); err != nil || installed {
		t.Fatalf("older topology accepted: installed=%v err=%v", installed, err)
	}
	if store.Get().Version != 5 {
		t.Fatalf("current version is %d, want 5", store.Get().Version)
	}
}

func TestTopologyRoundTripsThroughDisk(t *testing.T) {
	path := t.TempDir() + "/topology.json"
	topo, err := NewTopology(shardSet("s1", "s2"), 128, DefaultVNodes, DefaultEpsilon)
	if err != nil {
		t.Fatal(err)
	}
	if err := topo.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTopology(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("nothing loaded")
	}
	if loaded.Version != topo.Version || loaded.SlotCount != topo.SlotCount {
		t.Fatalf("round trip changed the map: %+v", loaded)
	}
	for slot := range topo.Slots {
		if loaded.Slots[slot] != topo.Slots[slot] {
			t.Fatalf("slot %d: %q != %q", slot, loaded.Slots[slot], topo.Slots[slot])
		}
	}
}

func TestLoadMissingTopologyIsNotAnError(t *testing.T) {
	loaded, err := LoadTopology(t.TempDir() + "/absent.json")
	if err != nil {
		t.Fatal(err)
	}
	if loaded != nil {
		t.Fatal("expected nil topology for a missing file")
	}
}
