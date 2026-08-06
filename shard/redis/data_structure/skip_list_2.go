package data_structure

import (
	"math/rand"
	"time"
)

const (
	MaxLevel    = 36
	Probability = 0.25
)

var defaultRand = rand.New(rand.NewSource(time.Now().UnixNano()))

type Element struct {
	Member string
	Score  int64
	Data   any
}

type node struct {
	element Element
	level   []*levelNode
}

type levelNode struct {
	forward *node
	span    uint64
}

type ISkipList interface {
	Insert(member string, score int64, data any) *Element
	Upsert(member string, delta int64, data any) *Element
	Delete(member string, score int64) bool
	GetRank(member string, score int64) int64
	GetByRank(rank int64) *Element
	GetElementByMember(member string) *Element
	UpdateScore(member string, newScore int64) bool
	GetRankRange(start, end int64) []*Element
	GetScoreRange(min, max int64) []*Element
	Len() uint64
}

// SkipList implementation
type skipList struct {
	head       *node            // head node, doesn't contain actual data
	tail       *node            // tail node
	length     uint64           // number of elements
	level      int              // current maximum level
	elementMap map[string]*node // mapping from member to node for fast lookup
}

func CreateSkipListRanking() map[string]ISkipList {
	return make(map[string]ISkipList)
}

// NewSkipList creates a new skip list
func NewSkipList() ISkipList {
	head := &node{
		level: make([]*levelNode, MaxLevel),
	}

	for i := 0; i < MaxLevel; i++ {
		head.level[i] = &levelNode{
			forward: nil,
			span:    0,
		}
	}

	return &skipList{
		head:       head,
		level:      1,
		elementMap: make(map[string]*node),
	}
}

// randomLevel generates a random level
func randomLevel() int {
	level := 1
	for level < MaxLevel && defaultRand.Float64() < Probability {
		level++
	}
	return level
}

// Insert inserts an element, or updates it if it already exists
func (sl *skipList) Insert(member string, score int64, data any) *Element {
	// If already exists, delete the old one first
	if oldNode, ok := sl.elementMap[member]; ok {
		sl.Delete(member, oldNode.element.Score)
	}

	// Create a new node
	level := randomLevel()
	if level > sl.level {
		sl.level = level
	}

	newNode := &node{
		element: Element{
			Member: member,
			Score:  score,
			Data:   data,
		},
		level: make([]*levelNode, level),
	}

	for i := 0; i < level; i++ {
		newNode.level[i] = &levelNode{
			forward: nil,
			span:    0,
		}
	}

	// Get insertion position
	var update [MaxLevel]*node
	var rank [MaxLevel]uint64

	// Find position
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// Note the comparison logic: higher scores come first, if scores are the same, sort by member ID lexicographically
		for x.level[i].forward != nil &&
			(x.level[i].forward.element.Score > score ||
				(x.level[i].forward.element.Score == score &&
					x.level[i].forward.element.Member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	// Insert the node
	for i := 0; i < level; i++ {
		newNode.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = newNode

		// Update spans
		newNode.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// Update spans for higher levels
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	// Update tail pointer if this is the last node
	if newNode.level[0].forward == nil {
		sl.tail = newNode
	}

	// Save to the map
	sl.elementMap[member] = newNode
	sl.length++

	return &newNode.element
}

// Upsert inserts an element, or updates it if it already exists
func (sl *skipList) Upsert(member string, delta int64, data any) *Element {
	n, ok := sl.elementMap[member]
	if !ok {
		return sl.Insert(member, delta, data)
	}
	newScore := n.element.Score + delta

	sl.Delete(member, n.element.Score)
	return sl.Insert(member, newScore, data)
}

// Delete removes an element
func (sl *skipList) Delete(member string, score int64) bool {
	// Find the node to delete
	var update [MaxLevel]*node

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		// Note the comparison logic: higher scores come first, if scores are the same, sort by member ID lexicographically
		for x.level[i].forward != nil &&
			(x.level[i].forward.element.Score > score ||
				(x.level[i].forward.element.Score == score &&
					x.level[i].forward.element.Member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	// Find the node to be deleted
	x = x.level[0].forward
	if x != nil && x.element.Score == score && x.element.Member == member {
		// Remove from all levels
		for i := 0; i < sl.level; i++ {
			if update[i].level[i].forward == x {
				update[i].level[i].span += x.level[i].span - 1
				update[i].level[i].forward = x.level[i].forward
			} else {
				update[i].level[i].span--
			}
		}

		// If deleted node was the tail
		if x.level[0].forward == nil {
			sl.tail = update[0]
		}

		// Update the maximum level
		for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
			sl.level--
		}

		// Remove from the map
		delete(sl.elementMap, member)
		sl.length--

		return true
	}

	return false
}

// GetRank gets the rank of a specified member, starting from 1 (rank 1 has the highest score)
func (sl *skipList) GetRank(member string, score int64) int64 {
	var rank uint64 = 0
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		// Note the comparison logic: higher scores come first, if scores are the same, sort by member ID lexicographically
		for x.level[i].forward != nil &&
			(x.level[i].forward.element.Score > score ||
				(x.level[i].forward.element.Score == score &&
					x.level[i].forward.element.Member < member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
	}

	x = x.level[0].forward
	if x != nil && x.element.Member == member {
		return int64(rank + 1)
	}

	return 0
}

// GetByRank gets an element by its rank, rank starts from 1
func (sl *skipList) GetByRank(rank int64) *Element {
	if rank <= 0 || rank > int64(sl.length) {
		return nil
	}

	var traversed uint64 = 0
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= uint64(rank) {
			traversed += x.level[i].span
			x = x.level[i].forward
		}

		if traversed == uint64(rank) {
			return &x.element
		}
	}

	return nil
}

// GetElementByMember gets an element by member name
func (sl *skipList) GetElementByMember(member string) *Element {
	if node, ok := sl.elementMap[member]; ok {
		return &node.element
	}
	return nil
}

// UpdateScore updates a member's score
func (sl *skipList) UpdateScore(member string, newScore int64) bool {
	if node, ok := sl.elementMap[member]; ok {
		oldScore := node.element.Score
		data := node.element.Data

		// Delete the old node and add a new one
		sl.Delete(member, oldScore)
		sl.Insert(member, newScore, data)
		return true
	}
	return false
}

// GetRankRange gets elements within a specified rank range
func (sl *skipList) GetRankRange(start, end int64) []*Element {
	var elements []*Element

	// Boundary check
	if start <= 0 {
		start = 1
	}

	if end > int64(sl.length) {
		end = int64(sl.length)
	}

	if start > end {
		return elements
	}

	// Get elements in the specified range
	for i := start; i <= end; i++ {
		element := sl.GetByRank(i)
		if element != nil {
			elements = append(elements, element)
		}
	}

	return elements
}

// GetScoreRange gets elements within a specified score range
func (sl *skipList) GetScoreRange(min, max int64) []*Element {
	var elements []*Element

	// Boundary check
	if min > max {
		return elements
	}

	// Find the first node whose score is in the range
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.element.Score > max {
			x = x.level[i].forward
		}
	}

	// Move to the first node that has a score <= max
	x = x.level[0].forward

	// Collect all nodes within the range
	for x != nil && x.element.Score >= min {
		elements = append(elements, &x.element)
		x = x.level[0].forward
	}

	return elements
}

// Len returns the number of elements in the skip list
func (sl *skipList) Len() uint64 {
	return sl.length
}
