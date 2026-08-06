package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

const shardReadyTimeout = 15 * time.Second

const migrateBatchSize = 512

type RebalanceRequest struct {
	Shards []*cluster.Shard

	DryRun bool

	MaxSlots int
}

type MigrationResult struct {
	Slots     []int    `json:"slots"`
	From      string   `json:"from"`
	To        string   `json:"to"`
	KeysMoved int      `json:"keys_moved"`
	Skipped   []string `json:"skipped_keys,omitempty"`
	Error     string   `json:"error,omitempty"`
}

type RebalanceReport struct {
	DryRun      bool           `json:"dry_run"`
	FromVersion int64          `json:"from_version"`
	ToVersion   int64          `json:"to_version"`
	Shards      []string       `json:"shards"`
	SlotsBefore map[string]int `json:"slots_before"`
	SlotsAfter  map[string]int `json:"slots_after"`

	PlannedMoves   int `json:"planned_moves"`
	MigratedSlots  int `json:"migrated_slots"`
	RemainingMoves int `json:"remaining_moves"`
	KeysMoved      int `json:"keys_moved"`
	SkippedKeys    int `json:"skipped_keys"`
	Failures       int `json:"failures"`

	Batches []MigrationResult `json:"batches,omitempty"`

	UnreachableNodes []string `json:"unreachable_nodes,omitempty"`
	Duration         string   `json:"duration"`

	Sample []cluster.Move `json:"sample_moves,omitempty"`
}

func (m *Manager) Rebalance(ctx context.Context, req RebalanceRequest) (*RebalanceReport, error) {
	if !m.rebalancing.TryLock() {
		return nil, fmt.Errorf("a rebalance is already running on %s", m.nodeID)
	}
	defer m.rebalancing.Unlock()

	started := time.Now()
	current := m.topo.Get()

	plan, err := cluster.ComputePlan(current, req.Shards)
	if err != nil {
		return nil, err
	}

	report := &RebalanceReport{
		DryRun:         req.DryRun,
		FromVersion:    current.Version,
		ToVersion:      current.Version,
		Shards:         plan.Shards,
		SlotsBefore:    plan.Before,
		SlotsAfter:     plan.After,
		PlannedMoves:   plan.MovedSlots,
		RemainingMoves: plan.MovedSlots,
	}

	if req.DryRun {
		report.Sample = plan.Moves
		if len(report.Sample) > 20 {
			report.Sample = report.Sample[:20]
		}
		report.Duration = time.Since(started).String()
		return report, nil
	}

	moves := plan.Moves
	if req.MaxSlots > 0 && len(moves) > req.MaxSlots {
		moves = moves[:req.MaxSlots]
	}

	announced, err := m.announceShards(plan.Target)
	if err != nil {
		return report, err
	}
	report.ToVersion = announced.Version
	report.UnreachableNodes = m.broadcastTopology(ctx, announced, plan.Target)

	for _, shardID := range newShardIDs(current, plan.Target) {
		if err := m.WaitShardLeader(ctx, shardID, shardReadyTimeout); err != nil {
			report.Duration = time.Since(started).String()
			return report, fmt.Errorf("new shard %s never became ready: %w", shardID, err)
		}
	}

	batches := batchMoves(moves, migrateBatchSize)
	report.Batches = make([]MigrationResult, 0, len(batches))
	for _, batch := range batches {
		result := m.migrateBatch(ctx, batch.From, batch.To, batch.Slots, plan.Target.SlotCount)
		report.MigratedSlots += len(batch.Slots)
		report.RemainingMoves -= len(batch.Slots)
		report.KeysMoved += result.KeysMoved
		report.SkippedKeys += len(result.Skipped)
		if result.Error != "" {
			report.Failures++
		}
		if result.KeysMoved > 0 || result.Error != "" || len(result.Skipped) > 0 {
			report.Batches = append(report.Batches, result)
		}
		if err := ctx.Err(); err != nil {
			report.Duration = time.Since(started).String()
			return report, fmt.Errorf("rebalance interrupted after %d/%d slots: %w",
				report.MigratedSlots, plan.MovedSlots, err)
		}
	}

	if report.RemainingMoves == 0 && report.Failures == 0 {
		final, err := m.retireShards(plan.Target)
		if err != nil {
			report.Duration = time.Since(started).String()
			return report, err
		}
		report.ToVersion = final.Version
		report.UnreachableNodes = m.broadcastTopology(ctx, final, plan.Target, current)
	}

	report.Duration = time.Since(started).String()
	return report, nil
}

func (m *Manager) announceShards(target *cluster.Topology) (*cluster.Topology, error) {
	return m.updateTopology(func(cur *cluster.Topology) (*cluster.Topology, error) {
		next := cur.Clone()
		next.Version++
		for id, shard := range target.Shards {
			next.Shards[id] = shard.Clone()
		}
		return next, nil
	})
}

func (m *Manager) retireShards(target *cluster.Topology) (*cluster.Topology, error) {
	return m.updateTopology(func(cur *cluster.Topology) (*cluster.Topology, error) {
		next := cur.Clone()
		next.Version++
		counts := next.SlotCounts()
		for id := range next.Shards {
			if _, wanted := target.Shards[id]; wanted {
				continue
			}
			if counts[id] == 0 {
				delete(next.Shards, id)
				log.Printf("[%s] retired shard %s: it owns no slots", m.nodeID, id)
			}
		}
		return next, nil
	})
}

func newShardIDs(cur, target *cluster.Topology) []string {
	var out []string
	for _, id := range target.ShardIDs() {
		if _, existed := cur.Shards[id]; !existed {
			out = append(out, id)
		}
	}
	return out
}

func (m *Manager) migrateBatch(ctx context.Context, from, to string, slots []int, slotCount int) MigrationResult {
	result := MigrationResult{Slots: slots, From: from, To: to}
	slotArgs := map[string]string{
		"slot_list": joinSlots(slots),
		"slots":     strconv.Itoa(slotCount),
	}

	migrations := make([]cluster.Migration, 0, len(slots))
	for _, slot := range slots {
		migrations = append(migrations, cluster.Migration{
			Slot: slot, From: from, To: to, State: cluster.MigrationCopying,
		})
	}
	marked, err := m.updateTopology(func(cur *cluster.Topology) (*cluster.Topology, error) {
		return cur.WithMigrations(migrations), nil
	})
	if err != nil {
		result.Error = fmt.Sprintf("mark migrating: %v", err)
		return result
	}
	m.broadcastTopology(ctx, marked)

	raw, _, err := m.shardCommand(ctx, from, &redis.Command{Cmd: redis.CmdDumpSlot, Args: slotArgs})
	if err != nil {
		result.Error = fmt.Sprintf("dump from %s: %v", from, err)
		m.abandonMigration(ctx, slots)
		return result
	}
	dump, err := decodeDump(raw)
	if err != nil {
		result.Error = fmt.Sprintf("decode dump from %s: %v", from, err)
		m.abandonMigration(ctx, slots)
		return result
	}
	result.KeysMoved = dump.KeyCount()
	for _, s := range dump.Skipped {
		result.Skipped = append(result.Skipped, s.Key+" ("+s.Type+")")
	}

	if dump.KeyCount() > 0 {
		payload, err := json.Marshal(dump)
		if err != nil {
			result.Error = fmt.Sprintf("encode dump: %v", err)
			m.abandonMigration(ctx, slots)
			return result
		}
		if _, _, err := m.shardCommand(ctx, to, &redis.Command{
			Cmd:  redis.CmdRestore,
			Args: map[string]string{"payload": string(payload)},
		}); err != nil {
			result.Error = fmt.Sprintf("restore into %s: %v", to, err)
			m.abandonMigration(ctx, slots)
			return result
		}
	}

	owners := make(map[int]string, len(slots))
	for _, slot := range slots {
		owners[slot] = to
	}
	flipped, err := m.updateTopology(func(cur *cluster.Topology) (*cluster.Topology, error) {
		return cur.WithSlotOwners(owners), nil
	})
	if err != nil {
		result.Error = fmt.Sprintf("flip ownership: %v", err)
		return result
	}
	m.broadcastTopology(ctx, flipped)

	if dump.KeyCount() > 0 {
		if _, _, err := m.shardCommand(ctx, from, &redis.Command{
			Cmd: redis.CmdDelSlot, Args: slotArgs,
		}); err != nil {
			result.Error = fmt.Sprintf("slots moved, but cleaning up %s failed: %v", from, err)
		}
	}
	return result
}

func (m *Manager) abandonMigration(ctx context.Context, slots []int) {
	reverted, err := m.updateTopology(func(cur *cluster.Topology) (*cluster.Topology, error) {
		owners := make(map[int]string, len(slots))
		for _, slot := range slots {
			owners[slot] = cur.Owner(slot)
		}
		return cur.WithSlotOwners(owners), nil
	})
	if err != nil {
		log.Printf("[%s] failed to clear migration markers on %d slot(s): %v", m.nodeID, len(slots), err)
		return
	}
	m.broadcastTopology(ctx, reverted)
}

func joinSlots(slots []int) string {
	parts := make([]string, len(slots))
	for i, slot := range slots {
		parts[i] = strconv.Itoa(slot)
	}
	return strings.Join(parts, ",")
}

func batchMoves(moves []cluster.Move, size int) []slotBatch {
	if size <= 0 {
		size = migrateBatchSize
	}
	var (
		batches []slotBatch
		index   = map[string]int{}
	)
	for _, move := range moves {
		key := move.From + "->" + move.To
		i, seen := index[key]
		if !seen || len(batches[i].Slots) >= size {
			batches = append(batches, slotBatch{From: move.From, To: move.To})
			i = len(batches) - 1
			index[key] = i
		}
		batches[i].Slots = append(batches[i].Slots, move.Slot)
	}
	return batches
}

type slotBatch struct {
	From  string
	To    string
	Slots []int
}

func decodeDump(raw any) (*redis.SlotDump, error) {
	if dump, ok := raw.(*redis.SlotDump); ok {
		return dump, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var dump redis.SlotDump
	if err := json.Unmarshal(encoded, &dump); err != nil {
		return nil, err
	}
	return &dump, nil
}
