package cluster

import (
	"fmt"
	"sort"
)

type Move struct {
	Slot int    `json:"slot"`
	From string `json:"from"`
	To   string `json:"to"`
}

type Plan struct {
	FromVersion int64          `json:"from_version"`
	Shards      []string       `json:"shards"`
	Moves       []Move         `json:"moves"`
	Before      map[string]int `json:"slots_before"`
	After       map[string]int `json:"slots_after"`

	MovedSlots int `json:"moved_slots"`

	Target *Topology `json:"-"`
}

func ComputePlan(cur *Topology, shards []*Shard) (*Plan, error) {
	if cur == nil {
		return nil, fmt.Errorf("cluster: no current topology")
	}
	if len(cur.Migrations) > 0 {
		return nil, fmt.Errorf("cluster: %d slot(s) still migrating; retry once they settle", len(cur.Migrations))
	}

	target, err := cur.WithShards(shards)
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		FromVersion: cur.Version,
		Shards:      target.ShardIDs(),
		Before:      cur.SlotCounts(),
		After:       target.SlotCounts(),
		Target:      target,
	}
	for slot := 0; slot < cur.SlotCount; slot++ {
		from, to := cur.Slots[slot], target.Slots[slot]
		if from != to {
			plan.Moves = append(plan.Moves, Move{Slot: slot, From: from, To: to})
		}
	}

	sort.SliceStable(plan.Moves, func(i, j int) bool {
		if plan.Moves[i].From != plan.Moves[j].From {
			return plan.Moves[i].From < plan.Moves[j].From
		}
		return plan.Moves[i].Slot < plan.Moves[j].Slot
	})
	plan.MovedSlots = len(plan.Moves)
	return plan, nil
}

func AddShard(t *Topology, shard *Shard) ([]*Shard, error) {
	if shard == nil || shard.ID == "" {
		return nil, fmt.Errorf("cluster: shard ID is required")
	}
	if len(shard.Members) == 0 {
		return nil, fmt.Errorf("cluster: shard %q needs at least one member", shard.ID)
	}
	if _, exists := t.Shards[shard.ID]; exists {
		return nil, fmt.Errorf("cluster: shard %q already exists", shard.ID)
	}
	out := make([]*Shard, 0, len(t.Shards)+1)
	for _, id := range t.ShardIDs() {
		out = append(out, t.Shards[id].Clone())
	}
	return append(out, shard.Clone()), nil
}

func RemoveShard(t *Topology, shardID string) ([]*Shard, error) {
	if _, exists := t.Shards[shardID]; !exists {
		return nil, fmt.Errorf("cluster: unknown shard %q", shardID)
	}
	if len(t.Shards) == 1 {
		return nil, fmt.Errorf("cluster: cannot remove the last shard")
	}
	out := make([]*Shard, 0, len(t.Shards)-1)
	for _, id := range t.ShardIDs() {
		if id == shardID {
			continue
		}
		out = append(out, t.Shards[id].Clone())
	}
	return out, nil
}
