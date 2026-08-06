package redis

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
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

// parseZScore parses a ZRANGEBYSCORE boundary: "(5", "+inf", "-inf", or plain float.
func parseZScore(s string) (float64, bool, error) {
	exclusive := false
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "+inf", "inf":
		return math.MaxFloat64, exclusive, nil
	case "-inf":
		return -math.MaxFloat64, exclusive, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, errors.New("(error) ERR min or max is not a float")
	}
	return v, exclusive, nil
}

func encodeZSetMembers(members []data_structure.ZSetMember, withScores bool) []byte {
	if len(members) == 0 {
		return RespEmptyArray
	}
	if withScores {
		out := make([]string, 0, len(members)*2)
		for _, m := range members {
			out = append(out, m.Member, fmt.Sprintf("%g", m.Score))
		}
		return Encode(out, false)
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, m.Member)
	}
	return Encode(out, false)
}

func (s *redisStore) cmdZINCRBY(args map[string]string) []byte {
	key := args["key"]
	member := args["member"]
	incStr := args["increment"]
	if key == "" || member == "" || incStr == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZINCRBY' command"), false)
	}
	delta, err := strconv.ParseFloat(incStr, 64)
	if err != nil {
		return Encode(errors.New("(error) ERR value is not a valid float"), false)
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		zset = data_structure.CreateZSet()
		s.zsetStore[key] = zset
	}
	newScore := zset.IncrBy(delta, member)
	return Encode(fmt.Sprintf("%g", newScore), false)
}

func (s *redisStore) cmdZREVRANK(args map[string]string) []byte {
	key := args["key"]
	member := args["member"]
	if key == "" || member == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZREVRANK' command"), false)
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}
	rank, _ := zset.GetRank(member, true)
	return Encode(rank, false)
}

// cmdZRANGE handles: ZRANGE key start stop [REV=true] [WITHSCORES=true]
func (s *redisStore) cmdZRANGE(args map[string]string) []byte {
	key := args["key"]
	startStr := args["start"]
	stopStr := args["stop"]
	if key == "" || startStr == "" || stopStr == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZRANGE' command"), false)
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return Encode(errors.New("(error) ERR value is not an integer"), false)
	}
	stop, err := strconv.ParseInt(stopStr, 10, 64)
	if err != nil {
		return Encode(errors.New("(error) ERR value is not an integer"), false)
	}
	reverse := strings.EqualFold(args["rev"], "true")
	withScores := strings.EqualFold(args["withscores"], "true")

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}
	return encodeZSetMembers(zset.RangeByRank(start, stop, reverse), withScores)
}

// cmdZREVRANGE is ZRANGE with REV=true (rank 0 = highest score).
func (s *redisStore) cmdZREVRANGE(args map[string]string) []byte {
	args["rev"] = "true"
	return s.cmdZRANGE(args)
}

// cmdZRANGEBYSCORE handles: ZRANGEBYSCORE key min max [WITHSCORES=true] [offset=N count=M]
func (s *redisStore) cmdZRANGEBYSCORE(args map[string]string) []byte {
	key := args["key"]
	minStr := args["min"]
	maxStr := args["max"]
	if key == "" || minStr == "" || maxStr == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZRANGEBYSCORE' command"), false)
	}
	minVal, minex, err := parseZScore(minStr)
	if err != nil {
		return Encode(err, false)
	}
	maxVal, maxex, err := parseZScore(maxStr)
	if err != nil {
		return Encode(err, false)
	}
	withScores := strings.EqualFold(args["withscores"], "true")
	offset := 0
	count := -1
	if v, ok := args["offset"]; ok {
		offset, _ = strconv.Atoi(v)
	}
	if v, ok := args["count"]; ok {
		count, _ = strconv.Atoi(v)
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}
	return encodeZSetMembers(zset.RangeByScore(minVal, maxVal, minex, maxex, offset, count, false), withScores)
}

// cmdZREVRANGEBYSCORE handles: ZREVRANGEBYSCORE key max min [WITHSCORES=true] [offset=N count=M]
func (s *redisStore) cmdZREVRANGEBYSCORE(args map[string]string) []byte {
	key := args["key"]
	minStr := args["min"]
	maxStr := args["max"]
	if key == "" || minStr == "" || maxStr == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZREVRANGEBYSCORE' command"), false)
	}
	minVal, minex, err := parseZScore(minStr)
	if err != nil {
		return Encode(err, false)
	}
	maxVal, maxex, err := parseZScore(maxStr)
	if err != nil {
		return Encode(err, false)
	}
	withScores := strings.EqualFold(args["withscores"], "true")
	offset := 0
	count := -1
	if v, ok := args["offset"]; ok {
		offset, _ = strconv.Atoi(v)
	}
	if v, ok := args["count"]; ok {
		count, _ = strconv.Atoi(v)
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}
	return encodeZSetMembers(zset.RangeByScore(minVal, maxVal, minex, maxex, offset, count, true), withScores)
}

func (s *redisStore) cmdZPOPMAX(args map[string]string) []byte {
	key := args["key"]
	if key == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZPOPMAX' command"), false)
	}
	count := 1
	if v, ok := args["count"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return Encode(errors.New("(error) ERR value is not an integer or out of range"), false)
		}
		count = n
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}
	members := zset.PopMax(count)
	if zset.Len() == 0 {
		delete(s.zsetStore, key)
	}
	return encodeZSetMembers(members, true)
}

func (s *redisStore) cmdZPOPMIN(args map[string]string) []byte {
	key := args["key"]
	if key == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZPOPMIN' command"), false)
	}
	count := 1
	if v, ok := args["count"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return Encode(errors.New("(error) ERR value is not an integer or out of range"), false)
		}
		count = n
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}
	members := zset.PopMin(count)
	if zset.Len() == 0 {
		delete(s.zsetStore, key)
	}
	return encodeZSetMembers(members, true)
}

func (s *redisStore) cmdZCOUNT(args map[string]string) []byte {
	key := args["key"]
	minStr := args["min"]
	maxStr := args["max"]
	if key == "" || minStr == "" || maxStr == "" {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'ZCOUNT' command"), false)
	}
	minVal, _, err := parseZScore(minStr)
	if err != nil {
		return Encode(err, false)
	}
	maxVal, _, err := parseZScore(maxStr)
	if err != nil {
		return Encode(err, false)
	}
	zset, exist := s.zsetStore[key]
	if !exist {
		return RespZero
	}
	return Encode(zset.Count(minVal, maxVal), false)
}
