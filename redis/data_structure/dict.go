package data_structure

import (
	"time"
)

const (
	KeyNumberLimit     = 5000000
	EvictStrategy      = EvictFirst
	EvictFirst     int = 0
)

type Obj struct {
	Value        any
	TypeEncoding uint8
}

type Dict struct {
	dictStore        map[string]*Obj
	expiredDictStore map[*Obj]int64
}

func CreateDict() *Dict {
	res := Dict{
		dictStore:        make(map[string]*Obj),
		expiredDictStore: make(map[*Obj]int64),
	}
	return &res
}

func (d *Dict) NewObj(value any, ttlMs int64, oType uint8, oEnc uint8) *Obj {
	obj := &Obj{
		Value:        value,
		TypeEncoding: oType | oEnc,
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
	return exp <= int64(time.Now().UnixMilli())
}

func (d *Dict) GetExpiry(obj *Obj) (int64, bool) {
	exp, exist := d.expiredDictStore[obj]
	return exp, exist
}

func (d *Dict) SetExpiry(obj *Obj, ttlMs int64) {
	d.expiredDictStore[obj] = int64(time.Now().UnixMilli()) + int64(ttlMs)
}

func (d *Dict) Get(k string) *Obj {
	v := d.dictStore[k]
	if v != nil {
		if d.HasExpired(v) {
			d.Del(k)
			return nil
		}
	}
	return v
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

func (d *Dict) evictFirst() {
	for k := range d.dictStore {
		d.Del(k)
		return
	}
}

func (d *Dict) evict() {
	switch EvictStrategy {
	case EvictFirst:
		d.evictFirst()
	default:
		d.evictFirst()
	}
}
