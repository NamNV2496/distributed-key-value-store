package redis

import (
	"errors"
	"strconv"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

func (s *redisStore) cmdSLAdd(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZADD' command"), false)
	}

	scoreStr, okScore := args["score"]
	score, err := strconv.ParseInt(scoreStr, 10, 64)
	if err != nil {
		return Encode(errors.New("Score must be interger"), false)
	}
	member, okMember := args["member"]
	data, okData := args["data"]
	if !okScore || !okMember || !okData {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZADD' command"), false)
	}
	skiplist, exist := s.skiplistStore[key]
	if !exist {
		skiplist = data_structure.NewSkipList()
		s.skiplistStore[key] = skiplist
	}
	skiplist.Upsert(member, score, data)
	return Encode("success", false)
}

func (s *redisStore) cmdSLGetRank(args map[string]string) []byte {
	key := args["key"]
	member := args["member"]

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(errors.New("key not found"), false)
	}

	elem := sl.GetElementByMember(member)
	if elem == nil {
		return Encode(errors.New("member not found"), false)
	}

	rank := sl.GetRank(member, elem.Score)

	return Encode(rank, false)
}

func (s *redisStore) cmdSLDelete(args map[string]string) []byte {
	key := args["key"]
	member := args["member"]

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(errors.New("key not found"), false)
	}

	elem := sl.GetElementByMember(member)
	if elem == nil {
		return Encode(errors.New("member not found"), false)
	}

	ok = sl.Delete(member, elem.Score)

	return Encode(ok, false)
}

func (s *redisStore) cmdSLGetByRank(args map[string]string) []byte {
	key := args["key"]

	rank, err := strconv.ParseInt(args["rank"], 10, 64)
	if err != nil {
		return Encode(err, false)
	}

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(errors.New("key not found"), false)
	}

	elem := sl.GetByRank(rank)
	if elem == nil {
		return Encode(nil, false)
	}

	return Encode(elem, true)
}

func (s *redisStore) cmdSLGetRankByName(args map[string]string) []byte {
	return s.cmdSLGetRank(args)
}

func (s *redisStore) cmdSLGetRankRange(args map[string]string) []byte {
	key := args["key"]

	start, err := strconv.ParseInt(args["start"], 10, 64)
	if err != nil {
		return Encode(err, false)
	}

	end, err := strconv.ParseInt(args["end"], 10, 64)
	if err != nil {
		return Encode(err, false)
	}

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(errors.New("key not found"), false)
	}

	return Encode(sl.GetRankRange(start, end), false)
}

func (s *redisStore) cmdSLGetScoreRang(args map[string]string) []byte {
	key := args["key"]

	min, err := strconv.ParseInt(args["min"], 10, 64)
	if err != nil {
		return Encode(err, false)
	}

	max, err := strconv.ParseInt(args["max"], 10, 64)
	if err != nil {
		return Encode(err, false)
	}

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(errors.New("key not found"), false)
	}

	return Encode(sl.GetScoreRange(min, max), false)
}

func (s *redisStore) cmdSLLen(args map[string]string) []byte {
	key := args["key"]

	sl, ok := s.skiplistStore[key]
	if !ok {
		return Encode(uint64(0), false)
	}

	return Encode(sl.Len(), false)
}
