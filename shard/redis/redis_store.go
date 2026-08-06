package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/namnv2496/go-redis-raft/shard/raft"
	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

const applyResultRing = 1024

type Command struct {
	Cmd  string            `json:"cmd,omitempty"`
	Args map[string]string `json:"args,omitempty"`
}

type IRedisStore interface {
	RunApplyLoop()
	EvalAndResponse(cmd *Command) (any, error)
	TakeResult(index int64) (any, error, bool)
}

type applyResult struct {
	index  int64
	result any
	err    error
	set    bool
}

type redisStore struct {
	raftNode      *raft.RaftNode
	dictStore     *data_structure.Dict
	zsetStore     map[string]data_structure.IZSet
	skiplistStore map[string]data_structure.ISkipList
	setStore      map[string]data_structure.ISet
	cmsStore      map[string]data_structure.ICMS
	bloomStore    map[string]data_structure.IBloomFilter
	rateLimiters  map[string]*rateLimiterState
	subscribers   map[string]*subscriber
	channelSubs   map[string]map[string]struct{}
	patternSubs   map[string]map[string]struct{}
	nextSubID     uint64
	mu            sync.RWMutex
	resultsMu     sync.Mutex
	results       [applyResultRing]applyResult
}

func NewRedisStoreWithEviction(raftNode *raft.RaftNode, evictStrategy int) IRedisStore {
	return &redisStore{
		dictStore:     data_structure.CreateDictWithEviction(evictStrategy),
		zsetStore:     data_structure.CreateZSetMap(),
		skiplistStore: data_structure.CreateSkipListRanking(),
		setStore:      data_structure.CreateSetMap(),
		cmsStore:      data_structure.CreateCMSMap(),
		bloomStore:    data_structure.CreateBloomFilterMap(),
		rateLimiters:  make(map[string]*rateLimiterState),
		subscribers:   make(map[string]*subscriber),
		channelSubs:   make(map[string]map[string]struct{}),
		patternSubs:   make(map[string]map[string]struct{}),
		raftNode:      raftNode,
		mu:            sync.RWMutex{},
		resultsMu:     sync.Mutex{},
		results:       [applyResultRing]applyResult{},
	}
}

func (s *redisStore) RunApplyLoop() {
	for entry := range s.raftNode.ApplyChan() {
		result, err := s.applyEntry(entry)
		if err != nil {
			log.Printf("apply loop: index %d (%s) failed: %v", entry.Index, entry.Cmd, err)
		}
		s.recordResult(entry.Index, result, err)
		s.raftNode.MarkApplied(entry.Index)
	}
}

func (s *redisStore) applyEntry(entry raft.LogEntry) (any, error) {
	if entry.Cmd == raft.NoOpCommand {
		return nil, nil
	}

	var cmd Command
	if len(entry.Data) > 0 {
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
	}
	if cmd.Cmd == "" {
		cmd.Cmd = entry.Cmd
	}
	if cmd.Cmd == "" {
		return nil, errors.New("missing command")
	}
	return s.EvalAndResponse(&cmd)
}

func (s *redisStore) recordResult(index int64, result any, err error) {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	s.results[index%applyResultRing] = applyResult{index: index, result: result, err: err, set: true}
}

func (s *redisStore) TakeResult(index int64) (any, error, bool) {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	slot := s.results[index%applyResultRing]
	if !slot.set || slot.index != index {
		return nil, nil, false
	}
	return slot.result, slot.err, true
}

func isReadOnlyCommand(cmd string) bool {
	switch cmd {
	case "PING", "GET", "TTL",
		"SCARD", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SRAND",
		"ZRANK", "ZREVRANK", "ZSCORE", "ZCARD", "ZRANGE", "ZREVRANGE",
		"ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZCOUNT",
		"GEODIST", "GEOHASH", "GEOSEARCH", "GEOPOS",
		"BF_INFO", "BF_EXISTS", "BF_MEXISTS", "CMS_QUERY",
		CmdDumpSlot:
		return true
	default:
		return false
	}
}

func (s *redisStore) EvalAndResponse(cmd *Command) (any, error) {
	if isReadOnlyCommand(cmd.Cmd) {
		s.mu.RLock()
		defer s.mu.RUnlock()
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	var res any
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
	// rate limit
	case "RATELIMIT_INIT":
		res = s.cmdRateLimitInit(cmd.Args)
	case "RATELIMIT_CHECK":
		res = s.cmdRateLimitCheck(cmd.Args)
	// pubsub
	case "PUBLISH":
		res = s.cmdPublish(cmd.Args)
	case "SUBSCRIBE":
		res = s.cmdSubscribe(cmd.Args)
	case "UNSUBSCRIBE":
		res = s.cmdUnsubscribe(cmd.Args)
	case "PSUBSCRIBE":
		res = s.cmdPSubscribe(cmd.Args)
	case "PUNSUBSCRIBE":
		res = s.cmdPUnsubscribe(cmd.Args)
	case "POLL":
		res = s.cmdPoll(cmd.Args)
	case "PUBSUB_CHANNELS":
		res = s.cmdPubSubChannels(cmd.Args)
	// skiplist for leaderboard
	case "SL_ADD":
		res = s.cmdSLAdd(cmd.Args)
	case "SL_GETRANK":
		res = s.cmdSLGetRank(cmd.Args)
	case "SL_DELETE":
		res = s.cmdSLDelete(cmd.Args)
	case "SL_GET_BY_RANK":
		res = s.cmdSLGetByRank(cmd.Args)
	case "SL_GET_RANK_BY_NAME":
		res = s.cmdSLGetRankByName(cmd.Args)
	case "SL_GET_RANK_RANGE":
		res = s.cmdSLGetRankRange(cmd.Args)
	case "SL_GET_SCORE_RANGE":
		res = s.cmdSLGetScoreRang(cmd.Args)
	case "SL_LEN":
		res = s.cmdSLLen(cmd.Args)
	case CmdDumpSlot:
		res = s.cmdClusterDumpSlot(cmd.Args)
	case CmdRestore:
		res = s.cmdClusterRestore(cmd.Args)
	case CmdDelSlot:
		res = s.cmdClusterDelSlot(cmd.Args)
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd.Cmd)
	}
	if err, ok := res.(error); ok {
		return nil, err
	}
	if resBytes, ok := res.([]byte); ok {
		return Decode(resBytes)
	}
	return res, nil
}
