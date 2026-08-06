package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	MigrationCopying = "copying"
	MigrationCleanup = "cleanup"
)

type Shard struct {
	ID string `json:"id"`

	Members map[string]string `json:"members"`
}

func (s *Shard) Clone() *Shard {
	out := &Shard{ID: s.ID, Members: make(map[string]string, len(s.Members))}
	for id, addr := range s.Members {
		out.Members[id] = addr
	}
	return out
}

func (s *Shard) NodeIDs() []string {
	ids := make([]string, 0, len(s.Members))
	for id := range s.Members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Shard) HasMember(nodeID string) bool {
	_, ok := s.Members[nodeID]
	return ok
}

type Migration struct {
	Slot  int    `json:"slot"`
	From  string `json:"from"`
	To    string `json:"to"`
	State string `json:"state"`
}

type Topology struct {
	Version    int64             `json:"version"`
	SlotCount  int               `json:"slot_count"`
	VNodes     int               `json:"vnodes"`
	Epsilon    float64           `json:"epsilon"`
	Shards     map[string]*Shard `json:"shards"`
	Slots      []string          `json:"slots"`
	Migrations []Migration       `json:"migrations,omitempty"`
}

func NewTopology(shards []*Shard, slotCount, vnodes int, epsilon float64) (*Topology, error) {
	if slotCount <= 0 {
		slotCount = DefaultSlotCount
	}
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	if epsilon <= 0 {
		epsilon = DefaultEpsilon
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("cluster: at least one shard is required")
	}

	t := &Topology{
		Version:   1,
		SlotCount: slotCount,
		VNodes:    vnodes,
		Epsilon:   epsilon,
		Shards:    make(map[string]*Shard, len(shards)),
	}
	ids := make([]string, 0, len(shards))
	for _, s := range shards {
		if s.ID == "" {
			return nil, fmt.Errorf("cluster: shard with empty ID")
		}
		if _, dup := t.Shards[s.ID]; dup {
			return nil, fmt.Errorf("cluster: duplicate shard %q", s.ID)
		}
		t.Shards[s.ID] = s.Clone()
		ids = append(ids, s.ID)
	}
	t.Slots = NewRing(ids, vnodes, epsilon).AssignSlots(slotCount)
	return t, t.Validate()
}

func (t *Topology) Clone() *Topology {
	out := &Topology{
		Version:   t.Version,
		SlotCount: t.SlotCount,
		VNodes:    t.VNodes,
		Epsilon:   t.Epsilon,
		Shards:    make(map[string]*Shard, len(t.Shards)),
		Slots:     make([]string, len(t.Slots)),
	}
	for id, s := range t.Shards {
		out.Shards[id] = s.Clone()
	}
	copy(out.Slots, t.Slots)
	if len(t.Migrations) > 0 {
		out.Migrations = make([]Migration, len(t.Migrations))
		copy(out.Migrations, t.Migrations)
	}
	return out
}

func (t *Topology) Validate() error {
	if t.SlotCount <= 0 {
		return fmt.Errorf("cluster: slot count must be positive, got %d", t.SlotCount)
	}
	if len(t.Slots) != t.SlotCount {
		return fmt.Errorf("cluster: slot table has %d entries, want %d", len(t.Slots), t.SlotCount)
	}
	if len(t.Shards) == 0 {
		return fmt.Errorf("cluster: topology has no shards")
	}
	for slot, owner := range t.Slots {
		if owner == "" {
			return fmt.Errorf("cluster: slot %d is unowned", slot)
		}
		if _, ok := t.Shards[owner]; !ok {
			return fmt.Errorf("cluster: slot %d owned by unknown shard %q", slot, owner)
		}
	}
	for id, s := range t.Shards {
		if id != s.ID {
			return fmt.Errorf("cluster: shard keyed %q but has ID %q", id, s.ID)
		}
		if len(s.Members) == 0 {
			return fmt.Errorf("cluster: shard %q has no members", id)
		}
	}
	return nil
}

func (t *Topology) Owner(slot int) string {
	if slot < 0 || slot >= len(t.Slots) {
		return ""
	}
	return t.Slots[slot]
}

func (t *Topology) Locate(key string) (slot int, shard *Shard) {
	slot = HashSlot(key, t.SlotCount)
	return slot, t.Shards[t.Owner(slot)]
}

func (t *Topology) ShardIDs() []string {
	ids := make([]string, 0, len(t.Shards))
	for id := range t.Shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (t *Topology) ShardsForNode(nodeID string) []*Shard {
	var out []*Shard
	for _, id := range t.ShardIDs() {
		if t.Shards[id].HasMember(nodeID) {
			out = append(out, t.Shards[id])
		}
	}
	return out
}

func (t *Topology) SlotCounts() map[string]int {
	counts := make(map[string]int, len(t.Shards))
	for id := range t.Shards {
		counts[id] = 0
	}
	for _, owner := range t.Slots {
		counts[owner]++
	}
	return counts
}

func (t *Topology) MigrationFor(slot int) *Migration {
	for i := range t.Migrations {
		if t.Migrations[i].Slot == slot {
			return &t.Migrations[i]
		}
	}
	return nil
}

func (t *Topology) WithMigration(m Migration) *Topology {
	out := t.Clone()
	out.Version++
	replaced := false
	for i := range out.Migrations {
		if out.Migrations[i].Slot == m.Slot {
			out.Migrations[i] = m
			replaced = true
			break
		}
	}
	if !replaced {
		out.Migrations = append(out.Migrations, m)
	}
	return out
}

func (t *Topology) WithMigrations(ms []Migration) *Topology {
	out := t.Clone()
	out.Version++
	index := make(map[int]int, len(out.Migrations))
	for i, m := range out.Migrations {
		index[m.Slot] = i
	}
	for _, m := range ms {
		if i, exists := index[m.Slot]; exists {
			out.Migrations[i] = m
			continue
		}
		index[m.Slot] = len(out.Migrations)
		out.Migrations = append(out.Migrations, m)
	}
	return out
}

func (t *Topology) WithSlotOwner(slot int, shardID string) *Topology {
	return t.WithSlotOwners(map[int]string{slot: shardID})
}

func (t *Topology) WithSlotOwners(owners map[int]string) *Topology {
	out := t.Clone()
	out.Version++
	for slot, shardID := range owners {
		if slot >= 0 && slot < len(out.Slots) && shardID != "" {
			out.Slots[slot] = shardID
		}
	}
	kept := out.Migrations[:0]
	for _, m := range out.Migrations {
		if _, touched := owners[m.Slot]; !touched {
			kept = append(kept, m)
		}
	}
	out.Migrations = kept
	if len(out.Migrations) == 0 {
		out.Migrations = nil
	}
	return out
}

func (t *Topology) WithShards(shards []*Shard) (*Topology, error) {
	next, err := NewTopology(shards, t.SlotCount, t.VNodes, t.Epsilon)
	if err != nil {
		return nil, err
	}
	next.Version = t.Version + 1
	return next, nil
}

func (t *Topology) Save(path string) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LoadTopology(path string) (*Topology, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var t Topology
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return &t, nil
}

type Store struct {
	mu   sync.RWMutex
	topo *Topology
	path string
}

func NewStore(topo *Topology, path string) *Store {
	return &Store{topo: topo, path: path}
}

func (s *Store) Get() *Topology {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topo
}

func (s *Store) Install(next *Topology) (bool, error) {
	if next == nil {
		return false, fmt.Errorf("cluster: nil topology")
	}
	if err := next.Validate(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topo != nil && next.Version <= s.topo.Version {
		return false, nil
	}
	s.topo = next
	return true, s.topo.Save(s.path)
}

func (s *Store) Update(fn func(*Topology) (*Topology, error)) (*Topology, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next, err := fn(s.topo)
	if err != nil {
		return nil, err
	}
	if err := next.Validate(); err != nil {
		return nil, err
	}
	if s.topo != nil && next.Version <= s.topo.Version {
		next.Version = s.topo.Version + 1
	}
	s.topo = next
	return next, s.topo.Save(s.path)
}
