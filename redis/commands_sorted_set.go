package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/namnv2496/go-redis-raft/redis/data_structure"
)

func (s *redisStore) cmdZADD(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZADD' command"), false)
	}

	score, okScore := args["score"]
	member, okMember := args["member"]
	if !okScore || !okMember {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZADD' command"), false)
	}

	flags := 0
	if nx, ok := args["nx"]; ok && strings.ToLower(nx) == "true" {
		flags |= data_structure.ZAddInNX
	}
	if xx, ok := args["xx"]; ok && strings.ToLower(xx) == "true" {
		flags |= data_structure.ZAddInXX
	}

	nx := (flags & data_structure.ZAddInNX) != 0
	xx := (flags & data_structure.ZAddInXX) != 0
	if nx && xx {
		return Encode(errors.New("(error) Cannot have both NX and XX flag for 'ZADD' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		zset = data_structure.CreateZSet()
		s.zsetStore[key] = zset
	}

	scoreVal, err := strconv.ParseFloat(score, 64)
	if err != nil {
		return Encode(errors.New("(error) Score must be floating point number"), false)
	}

	ret, outFlag := zset.Add(scoreVal, member, flags)
	if ret != 1 {
		return Encode(errors.New("error when adding element"), false)
	}

	count := 0
	if outFlag != data_structure.ZAddOutNop {
		count++
	}
	return Encode(count, false)
}

func (s *redisStore) cmdZRANK(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZRANK' command"), false)
	}
	member, ok := args["member"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZRANK' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}
	rank, _ := zset.GetRank(member, false)
	return Encode(rank, false)
}

func (s *redisStore) cmdZREM(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZREM' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespZero
	}

	deleted := 0
	for argKey, member := range args {
		if argKey != "key" {
			ret := zset.Del(member)
			if ret == 1 {
				deleted++
			}
			if zset.Len() == 0 {
				delete(s.zsetStore, key)
				break
			}
		}
	}
	return Encode(deleted, false)
}

func (s *redisStore) cmdZSCORE(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZSCORE' command"), false)
	}
	member, ok := args["member"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZSCORE' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}
	ret, score := zset.GetScore(member)
	if ret == 0 {
		return RespNil
	}
	return Encode(fmt.Sprintf("%f", score), false)
}

func (s *redisStore) cmdZCARD(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZCARD' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespZero
	}
	return Encode(zset.Len(), false)
}
