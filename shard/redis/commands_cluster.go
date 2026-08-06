package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/namnv2496/go-redis-raft/cluster"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

const (
	CmdDumpSlot = "CLUSTER_DUMPSLOT"
	CmdRestore  = "CLUSTER_RESTORE"
	CmdDelSlot  = "CLUSTER_DELSLOT"
)

type StringEntry struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	TypeEncoding uint8  `json:"type_encoding"`

	ExpireAtMs int64 `json:"expire_at_ms,omitempty"`
}

type SetEntry struct {
	Key     string   `json:"key"`
	Members []string `json:"members"`
}

type ZMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type ZSetEntry struct {
	Key     string    `json:"key"`
	Members []ZMember `json:"members"`
}

type SLMember struct {
	Member string `json:"member"`
	Score  int64  `json:"score"`
	Data   string `json:"data,omitempty"`
}

type SkipListEntry struct {
	Key     string     `json:"key"`
	Members []SLMember `json:"members"`
}

type SkippedEntry struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

type SlotDump struct {
	Slots     []int           `json:"slots"`
	SlotCount int             `json:"slot_count"`
	Strings   []StringEntry   `json:"strings,omitempty"`
	Sets      []SetEntry      `json:"sets,omitempty"`
	ZSets     []ZSetEntry     `json:"zsets,omitempty"`
	SkipLists []SkipListEntry `json:"skiplists,omitempty"`
	Skipped   []SkippedEntry  `json:"skipped,omitempty"`
}

func (d *SlotDump) KeyCount() int {
	return len(d.Strings) + len(d.Sets) + len(d.ZSets) + len(d.SkipLists)
}

func slotArgs(args map[string]string) (map[int]struct{}, int, error) {
	slotCount := cluster.DefaultSlotCount
	if s, ok := args["slots"]; ok {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return nil, 0, errors.New("ERR 'slots' is not a positive integer")
		}
		slotCount = n
	}

	raw := args["slot"]
	if list, ok := args["slot_list"]; ok && list != "" {
		if raw != "" {
			raw += ","
		}
		raw += list
	}
	if raw == "" {
		return nil, 0, errors.New("ERR missing 'slot' or 'slot_list' argument")
	}

	slots := make(map[int]struct{})
	for _, field := range strings.Split(raw, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		slot, err := strconv.Atoi(field)
		if err != nil {
			return nil, 0, fmt.Errorf("ERR %q is not a slot number", field)
		}
		if slot < 0 || slot >= slotCount {
			return nil, 0, fmt.Errorf("ERR slot %d out of range [0,%d)", slot, slotCount)
		}
		slots[slot] = struct{}{}
	}
	if len(slots) == 0 {
		return nil, 0, errors.New("ERR no slots named")
	}
	return slots, slotCount, nil
}

func sortedSlots(slots map[int]struct{}) []int {
	out := make([]int, 0, len(slots))
	for slot := range slots {
		out = append(out, slot)
	}
	sort.Ints(out)
	return out
}

func (s *redisStore) cmdClusterDumpSlot(args map[string]string) any {
	slots, slotCount, err := slotArgs(args)
	if err != nil {
		return err
	}
	dump := &SlotDump{Slots: sortedSlots(slots), SlotCount: slotCount}
	inSlot := func(key string) bool {
		_, ok := slots[cluster.HashSlot(key, slotCount)]
		return ok
	}

	for _, key := range s.dictStore.Keys() {
		if !inSlot(key) {
			continue
		}
		obj := s.dictStore.Get(key)
		if obj == nil {
			continue
		}
		entry := StringEntry{
			Key:          key,
			Value:        fmt.Sprint(obj.Value),
			TypeEncoding: obj.TypeEncoding,
		}
		if expireAt, hasTTL := s.dictStore.GetExpiry(obj); hasTTL {
			entry.ExpireAtMs = expireAt
		}
		dump.Strings = append(dump.Strings, entry)
	}

	for key, set := range s.setStore {
		if inSlot(key) {
			dump.Sets = append(dump.Sets, SetEntry{Key: key, Members: set.Members()})
		}
	}

	for key, zset := range s.zsetStore {
		if !inSlot(key) {
			continue
		}
		entry := ZSetEntry{Key: key}
		for _, m := range zset.RangeByRank(0, -1, false) {
			entry.Members = append(entry.Members, ZMember{Member: m.Member, Score: m.Score})
		}
		dump.ZSets = append(dump.ZSets, entry)
	}

	for key, sl := range s.skiplistStore {
		if !inSlot(key) {
			continue
		}
		entry := SkipListEntry{Key: key}
		for _, el := range sl.GetRankRange(1, int64(sl.Len())) {
			if el == nil {
				continue
			}
			member := SLMember{Member: el.Member, Score: el.Score}
			if el.Data != nil {
				member.Data = fmt.Sprint(el.Data)
			}
			entry.Members = append(entry.Members, member)
		}
		dump.SkipLists = append(dump.SkipLists, entry)
	}

	for key := range s.bloomStore {
		if inSlot(key) {
			dump.Skipped = append(dump.Skipped, SkippedEntry{Key: key, Type: "bloom"})
		}
	}
	for key := range s.cmsStore {
		if inSlot(key) {
			dump.Skipped = append(dump.Skipped, SkippedEntry{Key: key, Type: "cms"})
		}
	}
	for key := range s.rateLimiters {
		if inSlot(key) {
			dump.Skipped = append(dump.Skipped, SkippedEntry{Key: key, Type: "ratelimit"})
		}
	}

	return dump
}

func (s *redisStore) cmdClusterRestore(args map[string]string) any {
	payload, ok := args["payload"]
	if !ok {
		return errors.New("ERR missing 'payload' argument")
	}
	var dump SlotDump
	if err := json.Unmarshal([]byte(payload), &dump); err != nil {
		return fmt.Errorf("ERR invalid payload: %w", err)
	}

	now := time.Now().UnixMilli()
	restored := 0

	for _, entry := range dump.Strings {
		ttlMs := int64(NoExpire)
		if entry.ExpireAtMs > 0 {
			ttlMs = entry.ExpireAtMs - now
			if ttlMs <= 0 {
				continue
			}
		}
		oType := entry.TypeEncoding & 0x0F
		oEnc := entry.TypeEncoding & 0xF0
		s.dictStore.Put(entry.Key, s.dictStore.NewObj(entry.Value, ttlMs, oType, oEnc))
		restored++
	}

	for _, entry := range dump.Sets {
		set := data_structure.CreateSet(entry.Key)
		if len(entry.Members) > 0 {
			set.Add(entry.Members...)
		}
		s.setStore[entry.Key] = set
		restored++
	}

	for _, entry := range dump.ZSets {
		zset := data_structure.CreateZSet()
		for _, m := range entry.Members {
			zset.Add(m.Score, m.Member, 0)
		}
		s.zsetStore[entry.Key] = zset
		restored++
	}

	for _, entry := range dump.SkipLists {
		sl := data_structure.NewSkipList()
		for _, m := range entry.Members {
			sl.Insert(m.Member, m.Score, m.Data)
		}
		s.skiplistStore[entry.Key] = sl
		restored++
	}

	return int64(restored)
}

func (s *redisStore) cmdClusterDelSlot(args map[string]string) any {
	slots, slotCount, err := slotArgs(args)
	if err != nil {
		return err
	}
	inSlot := func(key string) bool {
		_, ok := slots[cluster.HashSlot(key, slotCount)]
		return ok
	}
	deleted := 0

	for _, key := range s.dictStore.Keys() {
		if inSlot(key) && s.dictStore.Del(key) {
			deleted++
		}
	}
	for key := range s.setStore {
		if inSlot(key) {
			delete(s.setStore, key)
			deleted++
		}
	}
	for key := range s.zsetStore {
		if inSlot(key) {
			delete(s.zsetStore, key)
			deleted++
		}
	}
	for key := range s.skiplistStore {
		if inSlot(key) {
			delete(s.skiplistStore, key)
			deleted++
		}
	}

	return int64(deleted)
}
