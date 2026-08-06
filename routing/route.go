package routing

import (
	"fmt"
	"net/http"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/redis"
)

var routingArgs = []string{"key", "channel", "pattern", "member", "id"}

type Decision struct {
	Slot    int
	ShardID string
	Keyless bool
}

type Error struct {
	Msg        string
	Status     int
	RetryAfter bool
}

func (e *Error) Error() string { return e.Msg }

func (e *Error) Status4xx() int {
	if e.Status == 0 {
		return http.StatusServiceUnavailable
	}
	return e.Status
}

func Route(
	topo *cluster.Topology,
	cmd *redis.Command,
	keyless func(*cluster.Topology) (string, bool),
) (Decision, error) {
	key, routed := routingKey(cmd)
	if !routed {
		shardID, ok := keyless(topo)
		if !ok {
			return Decision{Slot: -1}, &Error{
				Msg:    "no shard available to serve a command that names no key",
				Status: http.StatusServiceUnavailable,
			}
		}
		return Decision{Slot: -1, ShardID: shardID, Keyless: true}, nil
	}

	slot := cluster.HashSlot(key, topo.SlotCount)
	owner := topo.Owner(slot)
	if owner == "" {
		return Decision{Slot: slot}, &Error{
			Msg:    fmt.Sprintf("slot %d has no owner", slot),
			Status: http.StatusServiceUnavailable,
		}
	}
	if mig := topo.MigrationFor(slot); mig != nil && redis.IsWriteCommand(cmd.Cmd) {
		return Decision{Slot: slot, ShardID: owner}, &Error{
			Msg: fmt.Sprintf("slot %d is migrating from %s to %s; retry shortly",
				slot, mig.From, mig.To),
			Status:     http.StatusServiceUnavailable,
			RetryAfter: true,
		}
	}

	return Decision{Slot: slot, ShardID: owner}, nil
}

func routingKey(cmd *redis.Command) (string, bool) {
	for _, name := range routingArgs {
		if v, ok := cmd.Args[name]; ok && v != "" {
			return v, true
		}
	}
	return "", false
}

func FirstShard(topo *cluster.Topology) (string, bool) {
	ids := topo.ShardIDs()
	if len(ids) == 0 {
		return "", false
	}
	return ids[0], true
}
