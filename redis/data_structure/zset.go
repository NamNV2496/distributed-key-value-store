package data_structure

const ZAddInNX = 1 << 1 /* Only add new elements. Don't update already existing elements. */
const ZAddInXX = 1 << 2 /* Only update elements that already exist. Don't add new elements. */

const ZAddOutNop = 1 << 0     /* Operation not performed because of conditionals.*/
const ZAddOutAdded = 1 << 1   /* The element was new and was added. */
const ZAddOutUpdated = 1 << 2 /* The element already existed, score updated. */

type ZSetMember struct {
	Member string
	Score  float64
}

type IZSet interface {
	Add(score float64, ele string, flag int) (int, int)
	Del(ele string) int
	GetRank(ele string, reverse bool) (rank int64, score float64)
	GetScore(ele string) (int, float64)
	Len() int
	GetSkipList() *Skiplist
	IncrBy(delta float64, ele string) float64
	RangeByRank(start, stop int64, reverse bool) []ZSetMember
	RangeByScore(min, max float64, minex, maxex bool, offset, count int, reverse bool) []ZSetMember
	Count(min, max float64) int
	PopMax(count int) []ZSetMember
	PopMin(count int) []ZSetMember
}

var _ IZSet = &ZSet{}

type ZSet struct {
	zskiplist *Skiplist
	// map from ele to score
	dict map[string]float64
}

func CreateZSetMap() map[string]IZSet {
	return make(map[string]IZSet)
}

func CreateZSet() IZSet {
	zs := ZSet{
		zskiplist: CreateSkiplist(),
		dict:      map[string]float64{},
	}
	return &zs
}

func (zs *ZSet) Add(score float64, ele string, flag int) (int, int) {
	nx := flag & ZAddInNX
	xx := flag & ZAddInXX

	if len(ele) == 0 {
		return 0, ZAddOutNop
	}
	if curScore, exist := zs.dict[ele]; exist {
		if nx != 0 {
			return 1, ZAddOutNop
		}
		if curScore != score {
			znode := zs.zskiplist.UpdateScore(curScore, ele, score)
			zs.dict[ele] = znode.score
			return 1, ZAddOutUpdated
		}
		return 1, ZAddOutNop
	}

	// not exist
	if xx != 0 {
		return 1, ZAddOutNop
	}
	znode := zs.zskiplist.Insert(score, ele)
	zs.dict[ele] = znode.score
	return 1, ZAddOutAdded
}

/*
Return 1 if element existed and was deleted, 0 otherwise
*/
func (zs *ZSet) Del(ele string) int {
	score, exist := zs.dict[ele]
	if !exist {
		return 0
	}
	delete(zs.dict, ele)
	zs.zskiplist.Delete(score, ele)
	return 1
}

/*
Returns the 0-based rank of the object or -1 if the object does not exist.
If reverse is false, rank is computed considering as first element the one
with the lowest score. If reverse is true, rank is computed considering as element with rank 0 the
one with the highest score.
*/
func (zs *ZSet) GetRank(ele string, reverse bool) (rank int64, score float64) {
	setSize := zs.zskiplist.length
	score, exist := zs.dict[ele]
	if !exist {
		return -1, 0
	}
	rank = int64(zs.zskiplist.GetRank(score, ele))
	if reverse {
		rank = int64(setSize) - rank
	} else {
		rank--
	}
	return rank, score
}

// GetScore returns (1, score) when ele is present and (0, 0) when it is not.
//
// The flag used to be inverted — 0 meant found and -1 meant missing — while
// every caller tested it as "1 == found". The result was that ZSCORE returned
// nil for members that existed, and GEODIST, GEOHASH, GEOPOS and GEOSEARCH
// could never resolve a member at all.
func (zs *ZSet) GetScore(ele string) (int, float64) {
	score, exist := zs.dict[ele]
	if !exist {
		return 0, 0
	}
	return 1, score
}

func (zs *ZSet) Len() int {
	return len(zs.dict)
}

func (zs *ZSet) GetSkipList() *Skiplist {
	return zs.zskiplist
}

// IncrBy increments the score of ele by delta, inserting it if it doesn't exist.
// Returns the new score.
func (zs *ZSet) IncrBy(delta float64, ele string) float64 {
	if curScore, exist := zs.dict[ele]; exist {
		newScore := curScore + delta
		node := zs.zskiplist.UpdateScore(curScore, ele, newScore)
		zs.dict[ele] = node.score
		return newScore
	}
	node := zs.zskiplist.Insert(delta, ele)
	zs.dict[ele] = node.score
	return delta
}

// RangeByRank returns members at 0-based ranks [start, stop].
// Negative indices count from the end (-1 = last).
// If reverse is true, rank 0 is the highest score.
func (zs *ZSet) RangeByRank(start, stop int64, reverse bool) []ZSetMember {
	size := int64(zs.zskiplist.length)
	if size == 0 {
		return nil
	}
	if start < 0 {
		start += size
	}
	if stop < 0 {
		stop += size
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return nil
	}
	rangeLen := stop - start + 1

	if !reverse {
		node := zs.zskiplist.GetNodeByRank(uint32(start + 1))
		result := make([]ZSetMember, 0, rangeLen)
		for node != nil && int64(len(result)) < rangeLen {
			result = append(result, ZSetMember{Member: node.ele, Score: node.score})
			node = node.levels[0].forward
		}
		return result
	}

	// reverse: rank 0 = highest score; convert to ascending indices
	ascHigh := size - 1 - start
	ascLow := size - 1 - stop
	if ascLow < 0 {
		ascLow = 0
	}
	if ascHigh >= size {
		ascHigh = size - 1
	}
	node := zs.zskiplist.GetNodeByRank(uint32(ascHigh + 1))
	result := make([]ZSetMember, 0, rangeLen)
	for node != nil && int64(len(result)) < rangeLen {
		result = append(result, ZSetMember{Member: node.ele, Score: node.score})
		node = node.backward
	}
	return result
}

// RangeByScore returns members with scores in [min, max].
// offset/count implement LIMIT; count < 0 means unlimited.
// If reverse is true, results are returned highest-score-first.
func (zs *ZSet) RangeByScore(min, max float64, minex, maxex bool, offset, count int, reverse bool) []ZSetMember {
	zr := ZRange{min: min, max: max, minex: minex, maxex: maxex}
	var result []ZSetMember
	skipped := 0

	if !reverse {
		node := zs.zskiplist.FindFirstInRange(zr)
		for node != nil && zr.ValueLteMax(node.score) {
			if skipped < offset {
				skipped++
				node = node.levels[0].forward
				continue
			}
			result = append(result, ZSetMember{Member: node.ele, Score: node.score})
			if count >= 0 && len(result) >= count {
				break
			}
			node = node.levels[0].forward
		}
	} else {
		node := zs.zskiplist.FindLastInRange(zr)
		for node != nil && zr.ValueGteMin(node.score) {
			if skipped < offset {
				skipped++
				node = node.backward
				continue
			}
			result = append(result, ZSetMember{Member: node.ele, Score: node.score})
			if count >= 0 && len(result) >= count {
				break
			}
			node = node.backward
		}
	}
	return result
}

// Count returns the number of members with scores in [min, max].
func (zs *ZSet) Count(min, max float64) int {
	zr := ZRange{min: min, max: max}
	node := zs.zskiplist.FindFirstInRange(zr)
	count := 0
	for node != nil && zr.ValueLteMax(node.score) {
		count++
		node = node.levels[0].forward
	}
	return count
}

// PopMax removes and returns up to count members with the highest scores.
func (zs *ZSet) PopMax(count int) []ZSetMember {
	result := make([]ZSetMember, 0, count)
	for i := 0; i < count && zs.zskiplist.length > 0; i++ {
		tail := zs.zskiplist.tail
		if tail == nil {
			break
		}
		result = append(result, ZSetMember{Member: tail.ele, Score: tail.score})
		zs.Del(tail.ele)
	}
	return result
}

// PopMin removes and returns up to count members with the lowest scores.
func (zs *ZSet) PopMin(count int) []ZSetMember {
	result := make([]ZSetMember, 0, count)
	for i := 0; i < count && zs.zskiplist.length > 0; i++ {
		head := zs.zskiplist.head.levels[0].forward
		if head == nil {
			break
		}
		result = append(result, ZSetMember{Member: head.ele, Score: head.score})
		zs.Del(head.ele)
	}
	return result
}
