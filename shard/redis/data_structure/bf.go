package data_structure

import (
	"math"

	"github.com/spaolacci/murmur3"
)

const (
	ErrorTighteningRatio          = 0.5
	BfDefaultExpansion            = 2
	BfDefaultInitCapacity         = 100
	BfDefaultErrRate              = 0.01
	Ln2                   float64 = 0.693147180559945
	Ln2Square             float64 = 0.480453013918201
	ABigSeed              uint32  = 0x9747b28c
)

type IBloomFilter interface {
	CalcHash(entry string) HashValue
	Add(entry string)
	Exist(entry string) bool
	AddHash(initHash HashValue)
	ExistHash(initHash HashValue) bool

	// Introspection, used by BF.INFO.
	Capacity() uint64
	SizeBytes() uint64
	HashCount() int
	ErrorRate() float64
	Inserted() uint64
}

type BloomFilter struct {
	Hashes      int
	Entries     uint64
	Error       float64
	bitPerEntry float64
	bf          []uint8
	bits        uint64
	bytes       uint64
	inserted    uint64
}

// Capacity is the number of entries the filter was sized for.
func (b *BloomFilter) Capacity() uint64 { return b.Entries }

// SizeBytes is how much memory the bit array occupies.
func (b *BloomFilter) SizeBytes() uint64 { return b.bytes }

// HashCount is the number of hash functions per entry.
func (b *BloomFilter) HashCount() int { return b.Hashes }

// ErrorRate is the configured false-positive probability.
func (b *BloomFilter) ErrorRate() float64 { return b.Error }

// Inserted counts how many times an item has been added. Like Redis, this
// counts add operations, not distinct members — a Bloom filter cannot tell
// whether an item was already present.
func (b *BloomFilter) Inserted() uint64 { return b.inserted }

var _ IBloomFilter = &BloomFilter{}

type HashValue struct {
	a uint64
	b uint64
}

func calcBpe(err float64) float64 {
	num := math.Log(err)
	return math.Abs(-(num / Ln2Square))
}

func CreateBloomFilterMap() map[string]IBloomFilter {
	return make(map[string]IBloomFilter)
}

/*
http://en.wikipedia.org/wiki/Bloom_filter
- Optimal number of bits is: bits = (entries * ln(error)) / ln(2)^2
- bitPerEntry = bits/entries
- Optimal number of hash functions is: hashes = bitPerEntry * ln(2)
*/
func CreateBloomFilter(entries uint64, errorRate float64) IBloomFilter {
	bloom := BloomFilter{
		Entries: entries,
		Error:   errorRate,
	}
	bloom.bitPerEntry = calcBpe(errorRate)
	bits := uint64(float64(entries) * bloom.bitPerEntry)
	if bits%64 != 0 {
		bloom.bytes = ((bits / 64) + 1) * 8
	} else {
		bloom.bytes = bits / 8
	}
	bloom.bits = bloom.bytes * 8
	bloom.Hashes = int(math.Ceil(Ln2 * bloom.bitPerEntry))
	bloom.bf = make([]uint8, bloom.bytes)
	return &bloom
}

func (b *BloomFilter) CalcHash(entry string) HashValue {
	hasher := murmur3.New128WithSeed(ABigSeed)
	hasher.Write([]byte(entry))
	x, y := hasher.Sum128()
	return HashValue{
		a: x,
		b: y,
	}
}

func (b *BloomFilter) Add(entry string) {
	var hash, bytePos uint64
	b.inserted++
	initHash := b.CalcHash(entry)
	for i := 0; i < b.Hashes; i++ {
		hash = (initHash.a + initHash.b*uint64(i)) % b.bits
		bytePos = hash >> 3 // div 8
		b.bf[bytePos] |= 1 << (hash % 8)
	}
}

func (b *BloomFilter) Exist(entry string) bool {
	var hash, bytePos uint64
	initHash := b.CalcHash(entry)
	for i := 0; i < b.Hashes; i++ {
		hash = (initHash.a + initHash.b*uint64(i)) % b.bits
		bytePos = hash >> 3 // div 8
		if (b.bf[bytePos] & (1 << (hash % 8))) == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) AddHash(initHash HashValue) {
	var hash, bytePos uint64
	b.inserted++
	for i := 0; i < b.Hashes; i++ {
		hash = (initHash.a + initHash.b*uint64(i)) % b.bits
		bytePos = hash >> 3 // div 8
		b.bf[bytePos] |= 1 << (hash % 8)
	}
}

func (b *BloomFilter) ExistHash(initHash HashValue) bool {
	var hash, bytePos uint64
	for i := 0; i < b.Hashes; i++ {
		hash = (initHash.a + initHash.b*uint64(i)) % b.bits
		bytePos = hash >> 3 // div 8
		if (b.bf[bytePos] & (1 << (hash % 8))) == 0 {
			return false
		}
	}
	return true
}
