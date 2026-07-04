package redis

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/namnv2496/go-redis-raft/raft"
	"github.com/namnv2496/go-redis-raft/redis/data_structure"
)

// Command represents a Redis command serialized in Raft log
type Command struct {
	Cmd  string            `json:"cmd,omitempty"`
	Args map[string]string `json:"args,omitempty"`
}

// IRedisStore is the interface for the Redis store with Raft integration
type IRedisStore interface {
	RunApplyLoop()
	EvalAndResponse(cmd *Command) (any, error)
}

type redisStore struct {
	raftNode   *raft.RaftNode
	dictStore  *data_structure.Dict
	zsetStore  map[string]data_structure.IZSet
	setStore   map[string]data_structure.ISet
	cmsStore   map[string]data_structure.ICMS
	bloomStore map[string]data_structure.IBloomFilter
	mu         sync.RWMutex
}

func NewRedisStore(raftNode *raft.RaftNode) IRedisStore {
	return NewRedisStoreWithEviction(raftNode, data_structure.EvictFirst)
}

func NewRedisStoreWithEviction(raftNode *raft.RaftNode, evictStrategy int) IRedisStore {
	return &redisStore{
		dictStore:  data_structure.CreateDictWithEviction(evictStrategy),
		zsetStore:  data_structure.CreateZSetMap(),
		setStore:   data_structure.CreateSetMap(),
		cmsStore:   data_structure.CreateCMSMap(),
		bloomStore: data_structure.CreateBloomFilterMap(),
		raftNode:   raftNode,
	}
}

// RunApplyLoop reads committed entries from the Raft node and applies them to the store.
func (s *redisStore) RunApplyLoop() {
	for entry := range s.raftNode.ApplyChan() {
		var cmd Command
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			log.Printf("apply loop: unmarshal failed at index %d: %v", entry.Index, err)
			continue
		}
		if cmd.Cmd == "" {
			cmd.Cmd = entry.Cmd
		}
		if cmd.Cmd == "" {
			log.Printf("missing command: %s", cmd)
			continue
		}
		if _, err := s.EvalAndResponse(&cmd); err != nil {
			log.Printf("apply loop: %s at index %d failed: %v", cmd.Cmd, entry.Index, err)
		}
	}
}

func isReadOnlyCommand(cmd string) bool {
	switch cmd {
	case "PING", "GET", "TTL",
		"SCARD", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SRAND",
		"ZRANK", "ZREVRANK", "ZSCORE", "ZCARD", "ZRANGE", "ZREVRANGE",
		"ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZCOUNT",
		"GEODIST", "GEOHASH", "GEOSEARCH", "GEOPOS",
		"BF_INFO", "BF_EXISTS", "BF_MEXISTS", "CMS_QUERY":
		return true
	default:
		return false
	}
}

// EvalAndResponse executes cmd under the store lock.
// Safe to call concurrently from HTTP handlers and RunApplyLoop.
func (s *redisStore) EvalAndResponse(cmd *Command) (any, error) {
	if isReadOnlyCommand(cmd.Cmd) {
		s.mu.RLock()
		defer s.mu.RUnlock()
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	var res []byte
	switch cmd.Cmd {
	case "PING":
		res = s.cmdPING(cmd.Args)
	case "SET":
		res = s.cmdSET(cmd.Args)
	case "GET":
		res = s.cmdGET(cmd.Args)
	case "TTL":
		res = s.cmdGetTTL(cmd.Args)
	case "DEL":
		res = s.cmdDEL(cmd.Args)
	case "EXPIRE":
		res = s.cmdEXPIRE(cmd.Args)
	case "INCR":
		res = s.cmdINCR(cmd.Args)
	// Set
	case "SADD":
		res = s.cmdSADD(cmd.Args)
	case "SREM":
		res = s.cmdSREM(cmd.Args)
	case "SCARD":
		res = s.cmdSCARD(cmd.Args)
	case "SMEMBERS":
		res = s.cmdSMEMBERS(cmd.Args)
	case "SISMEMBER":
		res = s.cmdSISMEMBER(cmd.Args)
	case "SMISMEMBER":
		res = s.cmdSMISMEMBER(cmd.Args)
	case "SRAND":
		res = s.cmdSRAND(cmd.Args)
	case "SPOP":
		res = s.cmdSPOP(cmd.Args)
	// Sorted set
	case "ZADD":
		res = s.cmdZADD(cmd.Args)
	case "ZRANK":
		res = s.cmdZRANK(cmd.Args)
	case "ZREVRANK":
		res = s.cmdZREVRANK(cmd.Args)
	case "ZINCRBY":
		res = s.cmdZINCRBY(cmd.Args)
	case "ZREM":
		res = s.cmdZREM(cmd.Args)
	case "ZSCORE":
		res = s.cmdZSCORE(cmd.Args)
	case "ZCARD":
		res = s.cmdZCARD(cmd.Args)
	case "ZRANGE":
		res = s.cmdZRANGE(cmd.Args)
	case "ZREVRANGE":
		res = s.cmdZREVRANGE(cmd.Args)
	case "ZRANGEBYSCORE":
		res = s.cmdZRANGEBYSCORE(cmd.Args)
	case "ZREVRANGEBYSCORE":
		res = s.cmdZREVRANGEBYSCORE(cmd.Args)
	case "ZCOUNT":
		res = s.cmdZCOUNT(cmd.Args)
	case "ZPOPMAX":
		res = s.cmdZPOPMAX(cmd.Args)
	case "ZPOPMIN":
		res = s.cmdZPOPMIN(cmd.Args)
	// Geo hash
	case "GEOADD":
		res = s.cmdGEOADD(cmd.Args)
	case "GEODIST":
		res = s.cmdGEODIST(cmd.Args)
	case "GEOHASH":
		res = s.cmdGEOHASH(cmd.Args)
	case "GEOSEARCH":
		res = s.cmdGEOSEARCH(cmd.Args)
	case "GEOPOS":
		res = s.cmdGEOPOS(cmd.Args)
	// Bloom filter
	case "BF_RESERVE":
		res = s.cmdBFRESERVE(cmd.Args)
	case "BF_INFO":
		res = s.cmdBFINFO(cmd.Args)
	case "BF_MADD":
		res = s.cmdBFMADD(cmd.Args)
	case "BF_EXISTS":
		res = s.cmdBFEXISTS(cmd.Args)
	case "BF_MEXISTS":
		res = s.cmdBFMEXISTS(cmd.Args)
	// Count-Min Sketch
	case "CMS_INITBYDIM":
		res = s.cmdCMSINITBYDIM(cmd.Args)
	case "CMS_INITBYPROB":
		res = s.cmdCMSINITBYPROB(cmd.Args)
	case "CMS_INCRBY":
		res = s.cmdCMSINCRBY(cmd.Args)
	case "CMS_QUERY":
		res = s.cmdCMSQUERY(cmd.Args)
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd.Cmd)
	}
	return Decode(res)
}
