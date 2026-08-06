package redis

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

func (s *redisStore) cmdBFRESERVE(args map[string]string) []byte {
	if !(len(args) == 3 || len(args) == 5) {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'BF.RESERVE' command"), false)
	}
	key := args["key"]
	errRate, err := strconv.ParseFloat(args["errRate"], 64)
	if err != nil {
		return Encode(fmt.Errorf("error rate must be a floating point number %s", args["errRate"]), false)
	}
	capacity, err := strconv.ParseUint(args["capacity"], 10, 64)
	if err != nil {
		return Encode(fmt.Errorf("capacity must be an integer number %s", args["capacity"]), false)
	}
	if len(args) == 5 {
		if args["expansion"] != "EXPANSION" {
			return Encode(errors.New("(error) 4th param must be EXPANSION for 'BF.RESERVE' command"), false)
		}
		growthRate, err := strconv.ParseUint(args["growthRate"], 10, 32)
		if err != nil {
			return Encode(fmt.Errorf("growthRate must be an integer number %s", args["growthRate"]), false)
		}
		if growthRate < 1 {
			return Encode(fmt.Errorf("growthRate should be greater or equal to 1 %d", growthRate), false)
		}
	}
	_, exist := s.bloomStore[key]
	if exist {
		return Encode(fmt.Errorf("Bloom filter with key '%s' already exist", key), false)
	}
	s.bloomStore[key] = data_structure.CreateBloomFilter(capacity, errRate)
	return RespOk
}

func (s *redisStore) cmdBFINFO(args map[string]string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'BF.INFO' command"), false)
	}
	key := args["key"]
	bloom, exist := s.bloomStore[key]
	if !exist {
		return Encode(fmt.Errorf("Bloom filter with key '%s' does not exist", key), false)
	}

	res := []string{
		"Capacity", fmt.Sprintf("%d", bloom.Capacity()),
		"Size", fmt.Sprintf("%d", bloom.SizeBytes()),
		"Number of hashes", fmt.Sprintf("%d", bloom.HashCount()),
		"Error rate", strconv.FormatFloat(bloom.ErrorRate(), 'g', -1, 64),
		"Number of items inserted", fmt.Sprintf("%d", bloom.Inserted()),
	}
	return Encode(res, false)
}

func (s *redisStore) cmdBFMADD(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'BF.MADD' command"), false)
	}
	key := args["key"]
	bloom, exist := s.bloomStore[key]
	if !exist {
		bloom = data_structure.CreateBloomFilter(
			data_structure.BfDefaultInitCapacity,
			data_structure.BfDefaultErrRate,
		)
		s.bloomStore[key] = bloom
	}
	var res []string
	for i := 1; i < len(args); i++ {
		bloom.Add(args[strconv.Itoa(i)])
		res = append(res, "1")
	}
	return Encode(res, false)
}

func (s *redisStore) cmdBFEXISTS(args map[string]string) []byte {
	if len(args) != 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'BF.EXISTS' command"), false)
	}
	key, item := args["key"], args["item"]
	sb, exist := s.bloomStore[key]
	if !exist {
		return RespZero
	}
	if !sb.Exist(item) {
		return RespZero
	}
	return RespOne
}

func (s *redisStore) cmdBFMEXISTS(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'BF.MEXISTS' command"), false)
	}
	key := args["key"]
	sb, exist := s.bloomStore[key]
	var res []string
	for i := 1; i < len(args); i++ {
		if !exist {
			res = append(res, "0")
			continue
		}
		item := args[strconv.Itoa(i)]
		if !sb.Exist(item) {
			res = append(res, "0")
			continue
		}
		res = append(res, "1")
	}
	return Encode(res, false)
}
