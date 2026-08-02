package data_structure

import (
	"math/rand"
	"time"
)

const (
	KeyNumberLimit  = 5000000
	EvictSampleSize = 5 // number of keys sampled per eviction (mirrors Redis maxmemory-samples)
	lfuInitFreq     = 5 // initial frequency counter for new keys
	lfuLogFactor    = 10
	lfuDecayPerMin  = 1 // counter units lost per elapsed minute since last access
)

// Eviction policies.
const (
	EvictFirst int = iota // delete an arbitrary key (original behaviour)
	EvictLRU              // evict the least-recently-used key from a random sample
	EvictLFU              // evict the least-frequently-used key from a random sample
)

type Obj struct {
	Value        any
	TypeEncoding uint8
	lastAccessMs int64  // unix milliseconds; updated on every Get for LRU/LFU
	frequency    uint32 // logarithmic access counter; used by LFU
}

type Dict struct {
	dictStore        map[string]*Obj
	expiredDictStore map[*Obj]int64
	evictStrategy    int
}

func CreateDictWithEviction(strategy int) *Dict {
	return &Dict{
		dictStore:        make(map[string]*Obj),
		expiredDictStore: make(map[*Obj]int64),
		evictStrategy:    strategy,
	}
}

func (d *Dict) NewObj(value any, ttlMs int64, oType uint8, oEnc uint8) *Obj {
	obj := &Obj{
		Value:        value,
		TypeEncoding: oType | oEnc,
		lastAccessMs: time.Now().UnixMilli(),
		frequency:    lfuInitFreq,
	}
	if ttlMs > 0 {
		d.SetExpiry(obj, ttlMs)
	}
	return obj
}

func (d *Dict) HasExpired(obj *Obj) bool {
	exp, exist := d.expiredDictStore[obj]
	if !exist {
		return false
	}
	return exp <= time.Now().UnixMilli()
}

func (d *Dict) GetExpiry(obj *Obj) (int64, bool) {
	exp, exist := d.expiredDictStore[obj]
	return exp, exist
}

func (d *Dict) SetExpiry(obj *Obj, ttlMs int64) {
	d.expiredDictStore[obj] = time.Now().UnixMilli() + ttlMs
}

func (d *Dict) Get(k string) *Obj {
	v := d.dictStore[k]
	if v == nil {
		return nil
	}
	if d.HasExpired(v) {
		d.Del(k)
		return nil
	}
	d.touch(v)
	return v
}

// touch updates the access metadata used by LRU and LFU.
func (d *Dict) touch(obj *Obj) {
	now := time.Now().UnixMilli()
	switch d.evictStrategy {
	case EvictLRU:
		obj.lastAccessMs = now

	case EvictLFU:
		// Decay: subtract lfuDecayPerMin for every whole minute since last access.
		elapsed := (now - obj.lastAccessMs) / 60_000
		if elapsed > 0 {
			decay := uint32(elapsed) * lfuDecayPerMin
			if decay >= obj.frequency {
				obj.frequency = 0
			} else {
				obj.frequency -= decay
			}
		}
		obj.lastAccessMs = now

		// Logarithmic increment: probability 1 / (freq*lfuLogFactor + 1).
		// This makes the counter grow slowly at high frequencies (like Redis).
		if obj.frequency < 255 {
			p := 1.0 / (float64(obj.frequency)*float64(lfuLogFactor) + 1)
			if rand.Float64() < p {
				obj.frequency++
			}
		}
	}
}

func (d *Dict) Put(k string, obj *Obj) {
	if len(d.dictStore) >= KeyNumberLimit {
		d.evict()
	}
	d.dictStore[k] = obj
}

func (d *Dict) Del(k string) bool {
	if obj, exist := d.dictStore[k]; exist {
		delete(d.dictStore, k)
		delete(d.expiredDictStore, obj)
		return true
	}
	return false
}

func (d *Dict) evict() {
	switch d.evictStrategy {
	case EvictLRU:
		d.evictLRU()
	case EvictLFU:
		d.evictLFU()
	default:
		d.evictFirst()
	}
}

func (d *Dict) evictFirst() {
	for k := range d.dictStore {
		d.Del(k)
		return
	}
}

// evictLRU samples up to EvictSampleSize keys and removes the one accessed
// least recently.
func (d *Dict) evictLRU() {
	victim := ""
	var oldest int64 = -1
	sampled := 0
	for k, obj := range d.dictStore {
		if oldest < 0 || obj.lastAccessMs < oldest {
			oldest = obj.lastAccessMs
			victim = k
		}
		sampled++
		if sampled >= EvictSampleSize {
			break
		}
	}
	if victim != "" {
		d.Del(victim)
	}
}

func (d *Dict) evictLFU() {
	victim := ""
	lowestFreq := ^uint32(0)
	sampled := 0
	now := time.Now().UnixMilli()
	for k, obj := range d.dictStore {
		freq := obj.frequency
		elapsed := (now - obj.lastAccessMs) / 60_000
		if elapsed > 0 {
			decay := uint32(elapsed) * lfuDecayPerMin
			if decay >= freq {
				freq = 0
			} else {
				freq -= decay
			}
		}
		if freq < lowestFreq {
			lowestFreq = freq
			victim = k
		}
		sampled++
		if sampled >= EvictSampleSize {
			break
		}
	}
	if victim != "" {
		d.Del(victim)
	}
}
