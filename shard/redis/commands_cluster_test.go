package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

const testSlots = 64

func newTestStore(t *testing.T) *redisStore {
	t.Helper()
	return NewRedisStoreWithEviction(nil, data_structure.EvictFirst).(*redisStore)
}

func mustEval(t *testing.T, s *redisStore, cmd string, args map[string]string) any {
	t.Helper()
	res, err := s.EvalAndResponse(&Command{Cmd: cmd, Args: args})
	if err != nil {
		t.Fatalf("%s %v: %v", cmd, args, err)
	}
	return res
}

func keyInSlot(t *testing.T, slot int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("k%d", i)
		if cluster.HashSlot(key, testSlots) == slot {
			return key
		}
	}
	t.Fatalf("no key found for slot %d", slot)
	return ""
}

func dumpSlot(t *testing.T, s *redisStore, slot int) *SlotDump {
	t.Helper()
	res := mustEval(t, s, CmdDumpSlot, map[string]string{
		"slot":  strconv.Itoa(slot),
		"slots": strconv.Itoa(testSlots),
	})
	dump, ok := res.(*SlotDump)
	if !ok {
		t.Fatalf("DUMPSLOT returned %T, want *SlotDump", res)
	}
	return dump
}

func TestSlotDumpRestoreMovesEveryType(t *testing.T) {
	const slot = 11
	src, dst := newTestStore(t), newTestStore(t)

	strKey := keyInSlot(t, slot)
	setKey := strKey + "-set"
	zsetKey := strKey + "-zset"
	slKey := strKey + "-sl"

	setKey = "{" + strKey + "}set"
	zsetKey = "{" + strKey + "}zset"
	slKey = "{" + strKey + "}sl"

	mustEval(t, src, "SET", map[string]string{"key": strKey, "value": "hello"})
	mustEval(t, src, "SADD", map[string]string{"key": setKey, "value": "alpha"})
	mustEval(t, src, "ZADD", map[string]string{"key": zsetKey, "member": "player1", "score": "42"})
	mustEval(t, src, "SL_ADD", map[string]string{"key": slKey, "member": "p1", "score": "7", "data": "meta"})

	dump := dumpSlot(t, src, slot)
	if dump.KeyCount() != 4 {
		t.Fatalf("dump carried %d keys, want 4: %+v", dump.KeyCount(), dump)
	}

	payload, err := json.Marshal(dump)
	if err != nil {
		t.Fatal(err)
	}
	restored := mustEval(t, dst, CmdRestore, map[string]string{"payload": string(payload)})
	if restored != int64(4) {
		t.Fatalf("restored %v keys, want 4", restored)
	}

	if got := mustEval(t, dst, "GET", map[string]string{"key": strKey}); got != "hello" {
		t.Fatalf("GET after restore = %v, want hello", got)
	}
	if got := mustEval(t, dst, "SISMEMBER", map[string]string{"key": setKey, "value": "alpha"}); got != int64(1) {
		t.Fatalf("SISMEMBER after restore = %v, want 1", got)
	}
	if got := mustEval(t, dst, "ZSCORE", map[string]string{"key": zsetKey, "member": "player1"}); got != "42.000000" {
		t.Fatalf("ZSCORE after restore = %v, want 42", got)
	}
	if got := mustEval(t, dst, "SL_GETRANK", map[string]string{"key": slKey, "member": "p1"}); got != int64(1) {
		t.Fatalf("SL_GETRANK after restore = %v, want rank 1", got)
	}
}

func TestSlotDumpOnlyCoversItsOwnSlot(t *testing.T) {
	const slot = 3
	src := newTestStore(t)

	mine := keyInSlot(t, slot)
	other := keyInSlot(t, (slot+1)%testSlots)
	mustEval(t, src, "SET", map[string]string{"key": mine, "value": "in"})
	mustEval(t, src, "SET", map[string]string{"key": other, "value": "out"})

	dump := dumpSlot(t, src, slot)
	if len(dump.Strings) != 1 || dump.Strings[0].Key != mine {
		t.Fatalf("dump of slot %d picked up the wrong keys: %+v", slot, dump.Strings)
	}
}

func TestDelSlotRemovesOnlyThatSlot(t *testing.T) {
	const slot = 20
	src := newTestStore(t)

	mine := keyInSlot(t, slot)
	other := keyInSlot(t, (slot+7)%testSlots)
	mustEval(t, src, "SET", map[string]string{"key": mine, "value": "in"})
	mustEval(t, src, "SET", map[string]string{"key": other, "value": "out"})

	deleted := mustEval(t, src, CmdDelSlot, map[string]string{
		"slot":  strconv.Itoa(slot),
		"slots": strconv.Itoa(testSlots),
	})
	if deleted != int64(1) {
		t.Fatalf("DELSLOT removed %v keys, want 1", deleted)
	}
	if got, _ := src.EvalAndResponse(&Command{Cmd: "GET", Args: map[string]string{"key": mine}}); got != "" {
		t.Fatalf("migrated key still present: %v", got)
	}
	if got := mustEval(t, src, "GET", map[string]string{"key": other}); got != "out" {
		t.Fatalf("key in another slot was deleted: %v", got)
	}
}

func TestDumpSlotReportsUnmigratableKeys(t *testing.T) {
	const slot = 30
	src := newTestStore(t)

	bfKey := keyInSlot(t, slot)
	mustEval(t, src, "BF_RESERVE", map[string]string{"key": bfKey, "errRate": "0.01", "capacity": "100"})

	dump := dumpSlot(t, src, slot)
	if len(dump.Skipped) != 1 || dump.Skipped[0].Type != "bloom" {
		t.Fatalf("bloom filter not reported as skipped: %+v", dump.Skipped)
	}
}

func TestSlotCommandsRejectBadArguments(t *testing.T) {
	src := newTestStore(t)

	if _, err := src.EvalAndResponse(&Command{Cmd: CmdDumpSlot, Args: map[string]string{}}); err == nil {
		t.Fatal("expected DUMPSLOT without a slot to fail")
	}
	if _, err := src.EvalAndResponse(&Command{Cmd: CmdDumpSlot, Args: map[string]string{
		"slot": "99", "slots": "64",
	}}); err == nil {
		t.Fatal("expected an out-of-range slot to fail")
	}
	if _, err := src.EvalAndResponse(&Command{Cmd: CmdRestore, Args: map[string]string{
		"payload": "not json",
	}}); err == nil {
		t.Fatal("expected an invalid payload to fail")
	}
}
