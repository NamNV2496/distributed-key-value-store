package cluster

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
)

const DefaultVNodes = 160

const DefaultEpsilon = 0.05

type Ring struct {
	shards  []string
	vnodes  int
	epsilon float64
	points  []ringPoint
}

type ringPoint struct {
	hash  uint64
	shard string
}

func NewRing(shards []string, vnodes int, epsilon float64) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	if epsilon <= 0 {
		epsilon = DefaultEpsilon
	}

	seen := make(map[string]struct{}, len(shards))
	unique := make([]string, 0, len(shards))
	for _, s := range shards {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		unique = append(unique, s)
	}
	sort.Strings(unique)

	r := &Ring{
		shards:  unique,
		vnodes:  vnodes,
		epsilon: epsilon,
		points:  make([]ringPoint, 0, len(unique)*vnodes),
	}
	for _, shard := range unique {
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, ringPoint{
				hash:  hash64([]byte(fmt.Sprintf("%s#%d", shard, i))),
				shard: shard,
			})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].shard < r.points[j].shard
		}
		return r.points[i].hash < r.points[j].hash
	})
	return r
}

func (r *Ring) Shards() []string {
	out := make([]string, len(r.shards))
	copy(out, r.shards)
	return out
}

func (r *Ring) Lookup(slot int) string {
	if len(r.points) == 0 {
		return ""
	}
	idx := r.searchFrom(slotHash(slot))
	return r.points[idx].shard
}

func (r *Ring) AssignSlots(slotCount int) []string {
	if slotCount <= 0 {
		slotCount = DefaultSlotCount
	}
	assignment := make([]string, slotCount)
	if len(r.points) == 0 {
		return assignment
	}

	capacity := r.capacity(slotCount)
	load := make(map[string]int, len(r.shards))

	order := make([]int, slotCount)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ha, hb := slotHash(order[a]), slotHash(order[b])
		if ha == hb {
			return order[a] < order[b]
		}
		return ha < hb
	})

	for _, slot := range order {
		start := r.searchFrom(slotHash(slot))
		for step := 0; step < len(r.points); step++ {
			p := r.points[(start+step)%len(r.points)]
			if load[p.shard] < capacity {
				assignment[slot] = p.shard
				load[p.shard]++
				break
			}
		}
		if assignment[slot] == "" {

			p := r.points[start]
			assignment[slot] = p.shard
			load[p.shard]++
		}
	}
	return assignment
}

func (r *Ring) capacity(slotCount int) int {
	n := len(r.shards)
	if n == 0 {
		return slotCount
	}
	c := int(math.Ceil(float64(slotCount) / float64(n) * (1 + r.epsilon)))
	if c < 1 {
		c = 1
	}
	return c
}

func (r *Ring) searchFrom(hash uint64) int {
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= hash
	})
	if idx == len(r.points) {
		return 0
	}
	return idx
}

func slotHash(slot int) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(slot))
	return hash64(buf[:])
}

func hash64(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64()
}
