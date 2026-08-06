package redis

import (
	"strings"
	"testing"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

func newStore(t *testing.T) *redisStore {
	t.Helper()
	return NewRedisStoreWithEviction(nil, 0).(*redisStore)
}

// eval runs a command directly against the store, bypassing Raft.
func eval(t *testing.T, s *redisStore, cmd string, args map[string]string) any {
	t.Helper()
	res, err := s.EvalAndResponse(&Command{Cmd: cmd, Args: args})
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return res
}

// ---------------------------------------------------------------------------
// GetScore
// ---------------------------------------------------------------------------

// The found/missing flag was inverted: 0 meant found, -1 meant missing, while
// all five call sites tested for 1. Members that existed looked absent.
func TestZSetGetScoreFlag(t *testing.T) {
	zs := data_structure.CreateZSet()
	zs.Add(100, "alice", 0)

	if found, score := zs.GetScore("alice"); found != 1 || score != 100 {
		t.Fatalf("GetScore(present) = (%d, %v), want (1, 100)", found, score)
	}
	if found, score := zs.GetScore("nobody"); found != 0 || score != 0 {
		t.Fatalf("GetScore(absent) = (%d, %v), want (0, 0)", found, score)
	}
}

// ZSCORE returned nil for members that existed.
func TestZScoreFindsExistingMember(t *testing.T) {
	s := newStore(t)
	eval(t, s, "ZADD", map[string]string{"key": "board", "score": "100", "member": "alice"})

	got := eval(t, s, "ZSCORE", map[string]string{"key": "board", "member": "alice"})
	if got == "" || got == nil {
		t.Fatalf("ZSCORE returned %#v for a member ZCARD reports as present", got)
	}
	if !strings.HasPrefix(got.(string), "100") {
		t.Fatalf("ZSCORE = %v, want 100", got)
	}

	if missing := eval(t, s, "ZSCORE", map[string]string{"key": "board", "member": "nobody"}); missing != "" {
		t.Fatalf("ZSCORE of an absent member = %#v, want empty", missing)
	}
}

// GEODIST, GEOHASH, GEOPOS and GEOSEARCH all resolve members through
// GetScore, so all four were unable to find anything.
func TestGeoCommandsResolveMembers(t *testing.T) {
	s := newStore(t)
	eval(t, s, "GEOADD", map[string]string{
		"key": "cities", "longitude": "13.361389", "latitude": "38.115556", "member": "Palermo"})
	eval(t, s, "GEOADD", map[string]string{
		"key": "cities", "longitude": "15.087269", "latitude": "37.502669", "member": "Catania"})

	dist := eval(t, s, "GEODIST", map[string]string{
		"key": "cities", "member1": "Palermo", "member2": "Catania", "unit": "km"})
	if dist == "" || dist == nil {
		t.Fatal("GEODIST returned empty for two members that exist")
	}
	if !strings.HasPrefix(dist.(string), "16") { // ~166 km
		t.Fatalf("GEODIST = %v km, want roughly 166", dist)
	}

	hashes, ok := eval(t, s, "GEOHASH", map[string]string{"key": "cities", "1": "Palermo"}).([]any)
	if !ok || len(hashes) == 0 || hashes[0] == "" {
		t.Fatalf("GEOHASH = %#v, want a geohash string", hashes)
	}

	positions, ok := eval(t, s, "GEOPOS", map[string]string{"key": "cities", "1": "Palermo"}).([]any)
	if !ok || len(positions) == 0 {
		t.Fatalf("GEOPOS = %#v, want one coordinate pair", positions)
	}
	pair, ok := positions[0].([]any)
	if !ok || len(pair) != 2 {
		t.Fatalf("GEOPOS entry = %#v, want [lon, lat]", positions[0])
	}

	// GEOSEARCH used to fail outright with "could not decode requested zset
	// member". The radius is in metres, so 200 km == 200000.
	found, ok := eval(t, s, "GEOSEARCH", map[string]string{
		"key": "cities", "frommember": "Palermo", "radius": "200000"}).([]any)
	if !ok || len(found) != 2 {
		t.Fatalf("GEOSEARCH 200km = %#v, want both Palermo and Catania", found)
	}

	near, ok := eval(t, s, "GEOSEARCH", map[string]string{
		"key": "cities", "frommember": "Palermo", "radius": "200"}).([]any)
	if !ok || len(near) != 1 {
		t.Fatalf("GEOSEARCH 200m = %#v, want Palermo only", near)
	}
}

// ---------------------------------------------------------------------------
// DEL
// ---------------------------------------------------------------------------

// DEL iterated argument NAMES, so the documented {"key":"foo"} form deleted a
// key literally called "key", returned 0, and left foo in place.
func TestDelRemovesKeyNamedByKeyArgument(t *testing.T) {
	s := newStore(t)
	eval(t, s, "SET", map[string]string{"key": "foo", "value": "bar"})

	if got := eval(t, s, "DEL", map[string]string{"key": "foo"}); got != int64(1) {
		t.Fatalf("DEL {\"key\":\"foo\"} = %v, want 1", got)
	}
	if got := eval(t, s, "GET", map[string]string{"key": "foo"}); got != "" {
		t.Fatalf("foo still readable after DEL: %#v", got)
	}
}

func TestDelCountsOnlyKeysThatExisted(t *testing.T) {
	s := newStore(t)
	eval(t, s, "SET", map[string]string{"key": "present", "value": "v"})

	if got := eval(t, s, "DEL", map[string]string{"key": "absent"}); got != int64(0) {
		t.Fatalf("DEL of a missing key = %v, want 0", got)
	}
	if got := eval(t, s, "GET", map[string]string{"key": "present"}); got != "v" {
		t.Fatalf("deleting a missing key disturbed another key: %#v", got)
	}
}

func TestDelSupportsMultipleKeys(t *testing.T) {
	s := newStore(t)
	for _, k := range []string{"a", "b", "c"} {
		eval(t, s, "SET", map[string]string{"key": k, "value": "v"})
	}

	// key + numbered extras, the same variadic shape BF_MADD uses.
	got := eval(t, s, "DEL", map[string]string{"key": "a", "1": "b", "2": "missing"})
	if got != int64(2) {
		t.Fatalf("DEL a,b,missing = %v, want 2", got)
	}
	if v := eval(t, s, "GET", map[string]string{"key": "c"}); v != "v" {
		t.Fatalf("unrelated key c was removed: %#v", v)
	}
	for _, k := range []string{"a", "b"} {
		if v := eval(t, s, "GET", map[string]string{"key": k}); v != "" {
			t.Fatalf("key %s survived DEL: %#v", k, v)
		}
	}
}

func TestDelWithoutKeyIsAnError(t *testing.T) {
	s := newStore(t)
	if _, err := s.EvalAndResponse(&Command{Cmd: "DEL", Args: map[string]string{}}); err == nil {
		t.Fatal("DEL with no key should be an error")
	}
}

// ---------------------------------------------------------------------------
// BF_INFO
// ---------------------------------------------------------------------------

// BF_INFO's body was commented out, so it always returned an empty array.
func TestBloomInfoReportsFilterParameters(t *testing.T) {
	s := newStore(t)
	eval(t, s, "BF_RESERVE", map[string]string{"key": "bf", "errRate": "0.01", "capacity": "10000"})

	raw, ok := eval(t, s, "BF_INFO", map[string]string{"key": "bf"}).([]any)
	if !ok || len(raw) == 0 {
		t.Fatalf("BF_INFO = %#v, want name/value pairs", raw)
	}
	if len(raw)%2 != 0 {
		t.Fatalf("BF_INFO returned %d fields, want name/value pairs", len(raw))
	}

	info := make(map[string]string, len(raw)/2)
	for i := 0; i < len(raw); i += 2 {
		info[raw[i].(string)] = raw[i+1].(string)
	}
	if info["Capacity"] != "10000" {
		t.Errorf("Capacity = %q, want 10000", info["Capacity"])
	}
	if info["Error rate"] != "0.01" {
		t.Errorf("Error rate = %q, want 0.01", info["Error rate"])
	}
	if info["Number of items inserted"] != "0" {
		t.Errorf("items inserted = %q, want 0 before any add", info["Number of items inserted"])
	}
	if info["Size"] == "" || info["Size"] == "0" {
		t.Errorf("Size = %q, want the bit-array size in bytes", info["Size"])
	}
	if info["Number of hashes"] == "" || info["Number of hashes"] == "0" {
		t.Errorf("Number of hashes = %q, want a positive count", info["Number of hashes"])
	}

	// The insert counter must track additions.
	eval(t, s, "BF_MADD", map[string]string{"key": "bf", "1": "foo", "2": "bar"})
	raw2 := eval(t, s, "BF_INFO", map[string]string{"key": "bf"}).([]any)
	for i := 0; i < len(raw2); i += 2 {
		if raw2[i].(string) == "Number of items inserted" && raw2[i+1].(string) != "2" {
			t.Fatalf("items inserted after BF_MADD of 2 items = %q, want 2", raw2[i+1])
		}
	}
}

func TestBloomInfoOnMissingKeyIsAnError(t *testing.T) {
	s := newStore(t)
	if _, err := s.EvalAndResponse(&Command{Cmd: "BF_INFO", Args: map[string]string{"key": "nope"}}); err == nil {
		t.Fatal("BF_INFO on an unknown key should be an error")
	}
}
