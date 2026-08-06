package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/namnv2496/go-redis-raft/shard/redis/data_structure"
)

func (s *redisStore) cmdGEOADD(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOADD' command"), false)
	}

	lonStr, ok := args["longitude"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOADD' command"), false)
	}

	latStr, ok := args["latitude"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOADD' command"), false)
	}

	member, ok := args["member"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOADD' command"), false)
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		return Encode(fmt.Errorf("lon value must be a floating point number %s", lonStr), false)
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		return Encode(fmt.Errorf("lat value must be a floating point number %s", latStr), false)
	}

	hash, err := data_structure.GeohashEncode(data_structure.GeohashCoordRange, lon, lat, data_structure.GeoMaxStep)
	if err != nil {
		return Encode(err, false)
	}
	bits := data_structure.GeohashAlign52Bits(*hash)

	// Use ZADD to add the member with geohash score
	zaddArgs := map[string]string{
		"key":    key,
		"score":  fmt.Sprintf("%d", bits),
		"member": member,
	}
	return s.cmdZADD(zaddArgs)
}

/*
The distance is computed assuming that the Earth is a perfect sphere, so errors up to 0.5% are possible in edge cases.
*/
func (s *redisStore) cmdGEODIST(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEODIST' command"), false)
	}

	mem1, ok := args["member1"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEODIST' command"), false)
	}

	mem2, ok := args["member2"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEODIST' command"), false)
	}

	var unit float64 = 1
	if unitStr, ok := args["unit"]; ok {
		unitStr = strings.ToLower(unitStr)
		if unitStr == "km" {
			unit = 1000
		} else if unitStr == "ft" {
			unit = 0.3048
		} else if unitStr == "mi" {
			unit = 1609.34
		} else if unitStr != "m" {
			return Encode(errors.New("unsupported unit provided. please use M, KM, FT, MI"), false)
		}
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}
	exist1, score1 := zset.GetScore(mem1)
	if exist1 != 1 {
		return RespNil
	}
	exist2, score2 := zset.GetScore(mem2)
	if exist2 != 1 {
		return RespNil
	}

	score1GeohashBit := data_structure.GeohashBits{
		Step: data_structure.GeoMaxStep,
		Bits: uint64(score1),
	}
	lon1, lat1 := data_structure.GeohashDecodeAreaToLongLat(data_structure.GeohashCoordRange, score1GeohashBit)
	score2GeohashBit := data_structure.GeohashBits{
		Step: data_structure.GeoMaxStep,
		Bits: uint64(score2),
	}
	lon2, lat2 := data_structure.GeohashDecodeAreaToLongLat(data_structure.GeohashCoordRange, score2GeohashBit)
	dist := data_structure.GeohashGetDistance(lon1, lat1, lon2, lat2) / unit
	return Encode(fmt.Sprintf("%f", dist), false)
}

func (s *redisStore) cmdGEOHASH(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOHASH' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}

	var res []string
	for argKey, member := range args {
		if argKey != "key" {
			exist, score := zset.GetScore(member)
			if exist != 1 {
				res = append(res, "")
				continue
			}
			scoreGeohashBit := data_structure.GeohashBits{
				Step: data_structure.GeoMaxStep,
				Bits: uint64(score),
			}
			lon, lat := data_structure.GeohashDecodeAreaToLongLat(data_structure.GeohashCoordRange, scoreGeohashBit)
			/* The internal format we use for geocoding is a bit different
			 * than the standard, since we use as initial latitude range
			 * -85,85, while the normal geohashing algorithm uses -90,90.
			 * So we have to decode our position and re-encode using the
			 * standard ranges in order to output a valid geohash string.
			 */
			value, _ := data_structure.GeohashEncode(data_structure.GeohashStandardRange, lon, lat, data_structure.GeoMaxStep)
			value.Bits = data_structure.GeohashAlign52Bits(*value)
			hash := newBase32Encoding().Encode(value.Bits)
			res = append(res, hash)
		}
	}
	return Encode(res, false)
}

/*
GEOSEARCH key [FROMMEMBER member] [FROMLONLAT long lat] radius
TODO: support more options like Redis
*/
func (s *redisStore) cmdGEOSEARCH(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOSEARCH' command"), false)
	}

	radiusStr, ok := args["radius"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOSEARCH' command"), false)
	}

	var err error
	var ga []data_structure.GeoPoint
	var res []string
	var member string
	var long, lat float64
	fromMember := false

	if memberVal, ok := args["frommember"]; ok {
		member = memberVal
		fromMember = true
	} else if lonStr, ok := args["fromlonlat_lon"]; ok {
		if latStr, ok := args["fromlonlat_lat"]; ok {
			long, err = strconv.ParseFloat(lonStr, 64)
			if err != nil {
				return Encode(errors.New("(error) longitude must be a floating point number"), false)
			}
			lat, err = strconv.ParseFloat(latStr, 64)
			if err != nil {
				return Encode(errors.New("(error) latitude must be a floating point number"), false)
			}
		} else {
			return Encode(errors.New("(error) must provide both longitude and latitude"), false)
		}
	} else {
		return Encode(errors.New("(error) must provide FROMMEMBER or FROMLONLAT"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespEmptyArray
	}

	q := data_structure.GeohashCircularSearchQuery{}
	q.RadiusMeter, err = strconv.ParseFloat(radiusStr, 64)
	if err != nil {
		return Encode(errors.New("(error) radius must be a floating point number"), false)
	}

	if fromMember {
		memberExist, score := zset.GetScore(member)
		if memberExist != 1 {
			return Encode(errors.New("(error) could not decode requested zset member"), false)
		}
		hash := data_structure.GeohashBits{
			Step: data_structure.GeoMaxStep,
			Bits: uint64(score),
		}
		q.Long, q.Lat = data_structure.GeohashDecodeAreaToLongLat(data_structure.GeohashCoordRange, hash)
	} else {
		q.Long, q.Lat = long, lat
	}

	geohashRadius, err := data_structure.GeohashCalculateSearchingAreas(q)
	if err != nil {
		return Encode(err, false)
	}
	ga = data_structure.GeohashGetMemberOfAllNeighbors(zset.GetSkipList(), q, geohashRadius)
	for _, g := range ga {
		res = append(res, g.Member)
	}

	return Encode(res, false)
}

func (s *redisStore) cmdGEOPOS(args map[string]string) []byte {
	key, ok := args["key"]
	if !ok {
		return Encode(errors.New("(error) ERR wrong number of arguments for 'GEOPOS' command"), false)
	}

	zset, exist := s.zsetStore[key]
	if !exist {
		return RespNil
	}

	var res [][]string
	for argKey, member := range args {
		if argKey != "key" {
			exist, score := zset.GetScore(member)
			if exist != 1 {
				res = append(res, []string{})
				continue
			}
			hash := data_structure.GeohashBits{
				Step: data_structure.GeoMaxStep,
				Bits: uint64(score),
			}
			long, lat := data_structure.GeohashDecodeAreaToLongLat(data_structure.GeohashCoordRange, hash)
			res = append(res, []string{fmt.Sprintf("%f", long), fmt.Sprintf("%f", lat)})
		}
	}
	return Encode(res, false)
}
