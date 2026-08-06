package redis

import (
	"errors"
	"strconv"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

func (s *redisStore) cmdSADD(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SADD' command"), false)
	}
	key := args["key"] // TODO: check key is used by other types or not
	set, exist := s.setStore[key]
	if !exist {
		set = data_structure.CreateSet(key)
		s.setStore[key] = set
	}
	count := set.Add(args["value"])
	return Encode(count, false)
}

func (s *redisStore) cmdSREM(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SADD' command"), false)
	}
	key := args["key"]
	set, exist := s.setStore[key]
	if !exist {
		set = data_structure.CreateSet(key)
		s.setStore[key] = set
	}
	count := set.Rem(args["value"])
	return Encode(count, false)
}

func (s *redisStore) cmdSCARD(args map[string]string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SCARD' command"), false)
	}
	key := args["key"]
	set, exist := s.setStore[key]
	if !exist {
		return Encode(0, false)
	}
	return Encode(set.Size(), false)
}

func (s *redisStore) cmdSMEMBERS(args map[string]string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SMEMBERS' command"), false)
	}
	key := args["key"]
	set, exist := s.setStore[key]
	if !exist {
		return Encode(make([]string, 0), false)
	}
	return Encode(set.Members(), false)
}

func (s *redisStore) cmdSISMEMBER(args map[string]string) []byte {
	if len(args) != 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SISMEMBER' command"), false)
	}
	key := args["key"]
	set, exist := s.setStore[key]
	if !exist {
		return Encode(0, false)
	}
	return Encode(set.IsMember(args["value"]), false)
}

func (s *redisStore) cmdSMISMEMBER(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SMISMEMBER' command"), false)
	}
	key := args["key"]
	set, exist := s.setStore[key]
	if !exist {
		res := make([]int, len(args)-1)
		return Encode(res, false)
	}
	return Encode(set.MIsMember(args["value"]), false)
}

func (s *redisStore) cmdSPOP(args map[string]string) []byte {
	if len(args) > 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SPOP' command"), false)
	}
	key := args["key"]
	hasCount := len(args) > 1
	count := 0
	if hasCount {
		n, err := strconv.ParseInt(args["value"], 10, 64)
		if err != nil {
			return Encode(errors.New("(error) Count must be int"), false)
		}
		count = int(n)
	}

	set, exist := s.setStore[key]
	if !exist {
		if !hasCount {
			return Encode(nil, false)
		}
		return Encode(make([]string, 0), false)
	}
	if !hasCount {
		return Encode(set.Pop(count)[0], false)
	}
	return Encode(set.Pop(count), false)
}

func (s *redisStore) cmdSRAND(args map[string]string) []byte {
	if len(args) > 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SRAND' command"), false)
	}
	key := args["key"]
	hasCount := len(args) > 1
	count := 0
	if hasCount {
		n, err := strconv.ParseInt(args["value"], 10, 64)
		if err != nil {
			return Encode(errors.New("(error) Count must be int"), false)
		}
		count = int(n)
	}

	set, exist := s.setStore[key]
	if !exist {
		if !hasCount {
			return Encode(nil, false)
		}
		return Encode(make([]string, 0), false)
	}
	if !hasCount {
		return Encode(set.Rand(count)[0], false)
	}
	return Encode(set.Rand(count), false)
}
