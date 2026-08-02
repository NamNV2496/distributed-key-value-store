package redis

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/namnv2496/go-redis-raft/redis/data_structure"
)

func (s *redisStore) cmdCMSINITBYDIM(args map[string]string) []byte {
	if len(args) != 3 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'CMS.INITBYDIM' command"), false)
	}
	key := args["key"]
	width, err := strconv.ParseUint(args["width"], 10, 32)
	if err != nil {
		return Encode(fmt.Errorf("width must be a integer number %s", args["width"]), false)
	}
	height, err := strconv.ParseUint(args["height"], 10, 32)
	if err != nil {
		return Encode(fmt.Errorf("height must be a integer number %s", args["height"]), false)
	}
	_, exist := s.cmsStore[key]
	if exist {
		return Encode(errors.New("CMS: key already exists"), false)
	}
	s.cmsStore[key] = data_structure.CreateCMS(uint32(width), uint32(height))
	return RespOk
}

func (s *redisStore) cmdCMSINITBYPROB(args map[string]string) []byte {
	if len(args) != 3 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'CMS.INITBYPROB' command"), false)
	}
	key := args["key"]
	errRate, err := strconv.ParseFloat(args["errRate"], 64)
	if err != nil {
		return Encode(fmt.Errorf("errRate must be a floating point number %s", args["errRate"]), false)
	}
	if errRate >= 1 || errRate <= 0 {
		return Encode(errors.New("CMS: invalid overestimation value"), false)
	}
	probability, err := strconv.ParseFloat(args["probability"], 64)
	if err != nil {
		return Encode(fmt.Errorf("probability must be a floating poit number %s", args["probability"]), false)
	}
	if probability >= 1 || probability <= 0 {
		return Encode(errors.New("CMS: invalid prob value"), false)
	}
	cms, exist := s.cmsStore[key]
	if exist {
		return Encode(errors.New("CMS: key already exists"), false)
	}
	w, h := cms.CalcCMSDim(errRate, probability)
	s.cmsStore[key] = data_structure.CreateCMS(w, h)
	return RespOk
}

func (s *redisStore) cmdCMSINCRBY(args map[string]string) []byte {
	if len(args) < 3 || len(args)%2 == 0 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'CMS.INCBY' command"), false)
	}
	key := args["key"]
	cms, exist := s.cmsStore[key]
	if !exist {
		return Encode(errors.New("CMS: key does not exist"), false)
	}
	var res []string

	value, err := strconv.ParseUint(args["value"], 10, 32)
	if err != nil {
		return Encode(fmt.Errorf("increment must be a non negative integer number %s", args["value"]), false)
	}
	count := cms.IncrBy(key, uint32(value))
	if count == math.MaxUint32 {
		res = append(res, "CMS: INCRBY overflow")
	}
	res = append(res, fmt.Sprintf("%d", count))
	return Encode(res, false)
}

func (s *redisStore) cmdCMSQUERY(args map[string]string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'CMS.QUERY' command"), false)
	}
	key := args["key"]
	cms, exist := s.cmsStore[key]
	if !exist {
		return Encode(errors.New("CMS: key does not exist"), false)
	}
	var res []string
	res = append(res, fmt.Sprintf("%d", cms.Count(key)))
	return Encode(res, false)
}
