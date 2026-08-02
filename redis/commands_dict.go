package redis

import (
	"errors"
	"strconv"
	"time"
)

func (s *redisStore) cmdPING(args map[string]string) []byte {
	var buf []byte

	if len(args) > 1 {
		return Encode(errors.New("ERR wrong number of arguments for 'PING' command"), false)
	}

	if len(args) == 0 {
		buf = Encode("PONG", true)
	} else if msg, ok := args["message"]; ok {
		buf = Encode(msg, false)
	} else {
		buf = Encode("PONG", true)
	}

	return buf
}

func (s *redisStore) cmdSET(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SET' command"), false)
	}
	value, ok := args["value"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'SET' command"), false)
	}

	var ttlMs int64 = -1
	oType, oEnc := deduceTypeString(value)

	if ttlStr, ok := args["ttl"]; ok {
		ttlSec, err := strconv.ParseInt(ttlStr, 10, 64)
		if err != nil {
			return Encode(errors.New("(error) ERR value is not an integer or out of range"), false)
		}
		ttlMs = ttlSec * 1000
	}

	s.dictStore.Put(key, s.dictStore.NewObj(value, ttlMs, oType, oEnc))
	return RespOk
}

func (s *redisStore) cmdGET(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GET' command"), false)
	}

	obj := s.dictStore.Get(key)
	if obj == nil {
		return RespNil
	}

	if s.dictStore.HasExpired(obj) {
		return RespNil
	}

	return Encode(obj.Value, false)
}

func (s *redisStore) cmdGetTTL(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'TTL' command"), false)
	}

	obj := s.dictStore.Get(key)
	if obj == nil {
		return TtlKeyNotExist
	}

	exp, isExpirySet := s.dictStore.GetExpiry(obj)
	if !isExpirySet {
		return TtlKeyExistNoExpire
	}

	remainMs := exp - int64(time.Now().UnixMilli())
	if remainMs < 0 {
		return TtlKeyNotExist
	}

	return Encode(int64(remainMs/1000), false)
}

func (s *redisStore) cmdDEL(args map[string]string) []byte {
	keys := make([]string, 0, len(args))
	if key, ok := args["key"]; ok && key != "" {
		keys = append(keys, key)
	}
	for i := 1; ; i++ {
		key, ok := args[strconv.Itoa(i)]
		if !ok {
			break
		}
		if key != "" {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'DEL' command"), false)
	}

	delCount := 0
	for _, key := range keys {
		if ok := s.dictStore.Del(key); ok {
			delCount++
		}
	}

	return Encode(delCount, false)
}

func (s *redisStore) cmdEXPIRE(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'EXPIRE' command"), false)
	}

	ttlStr, ok := args["ttl"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'EXPIRE' command"), false)
	}

	ttlSec, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return Encode(errors.New("(error) ERR value is not an integer or out of range"), false)
	}

	obj := s.dictStore.Get(key)
	if obj == nil {
		return RespZero
	}

	s.dictStore.SetExpiry(obj, ttlSec*1000)
	return RespOne
}

func (s *redisStore) cmdINCR(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'INCR' command"), false)
	}

	obj := s.dictStore.Get(key)
	if obj == nil {
		obj = s.dictStore.NewObj("0", NoExpire, ObjTypeString, ObjEncodingInt)
		s.dictStore.Put(key, obj)
	}

	if err := assertType(obj.TypeEncoding, ObjTypeString); err != nil {
		return Encode(err, false)
	}

	if err := assertEncoding(obj.TypeEncoding, ObjEncodingInt); err != nil {
		return Encode(err, false)
	}

	i, _ := strconv.ParseInt(obj.Value.(string), 10, 64)
	i++
	obj.Value = strconv.FormatInt(i, 10)

	return Encode(i, false)
}
