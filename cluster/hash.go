package cluster

import (
	"hash/crc32"
	"strings"
)

const DefaultSlotCount = 16384

func HashSlot(key string, slotCount int) int {
	if slotCount <= 0 {
		slotCount = DefaultSlotCount
	}
	return int(crc32.ChecksumIEEE([]byte(HashTag(key))) % uint32(slotCount))
}

func HashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	close := strings.IndexByte(key[open+1:], '}')
	if close <= 0 {
		return key
	}
	return key[open+1 : open+1+close]
}
