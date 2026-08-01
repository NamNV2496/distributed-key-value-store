package redis

import (
	"bytes"
	"errors"
	"fmt"
)

type CommandOP string

const (
	CRLF string = "\r\n"
)

var (
	RespNil             = []byte("$-1\r\n")
	RespOk              = []byte("+OK\r\n")
	RespZero            = []byte(":0\r\n")
	RespOne             = []byte(":1\r\n")
	RespEmptyArray      = []byte("*0\r\n")
	TtlKeyNotExist      = []byte(":-2\r\n")
	TtlKeyExistNoExpire = []byte(":-1\r\n")
)

const (
	NoExpire          int64 = -1
	ObjTypeString     uint8 = 0
	ObjEncodingInt    uint8 = 0
	ObjEncodingEmbStr uint8 = 8
)

func deduceTypeString(value string) (uint8, uint8) {
	// Simple type deduction - string type with embstr encoding for short strings
	return ObjTypeString, ObjEncodingEmbStr
}

func assertType(te uint8, expectedType uint8) error {
	actualType := te & 0x0F
	if actualType != expectedType {
		return errors.New("ERR operation against a key holding the wrong kind of value")
	}
	return nil
}

func assertEncoding(te uint8, expectedEncoding uint8) error {
	actualEncoding := te & 0xF0
	if actualEncoding != expectedEncoding {
		return errors.New("ERR operation against a key holding the wrong kind of value")
	}
	return nil
}

// Base32Encoding is a simple base32 encoding implementation
var Base32Encoding = struct {
	Encode func(uint64) string
}{
	Encode: func(bits uint64) string {
		const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUV"
		var result []byte
		for i := 0; i < 13; i++ { // 52 bits / 4 = 13 chars (using 5 bits each)
			result = append(result, alphabet[bits&0x1F])
			bits >>= 5
		}
		return string(result)
	},
}

func DecodeOne(data []byte) (any, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("no data")
	}
	switch data[0] {
	case '+':
		return readSimpleString(data)
	case ':':
		return readInt64(data)
	case '-':
		return readError(data)
	case '$':
		return readBulkString(data)
	case '*':
		return readArray(data)
	case '@':
		return readIntArray(data)
	default:
		return readSimpleString(data)
	}
}

func Decode(data []byte) (any, error) {
	res, _, err := DecodeOne(data)
	if err != nil {
		return nil, err
	}
	if e, ok := res.(error); ok {
		return nil, e
	}
	return res, nil
}

func encodeString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}
func encodeStringArray(sa []string) []byte {
	var b []byte
	buf := bytes.NewBuffer(b)
	for _, s := range sa {
		buf.Write(encodeString(s))
	}
	return []byte(fmt.Sprintf("*%d\r\n%s", len(sa), buf.Bytes()))
}

func Encode(value any, isSimpleString bool) []byte {
	switch v := value.(type) {
	case string:
		if isSimpleString {
			return []byte(fmt.Sprintf("+%s%s", v, CRLF))
		}
		return []byte(fmt.Sprintf("$%d%s%s%s", len(v), CRLF, v, CRLF))
	case int64, int32, int16, int8, int:
		return []byte(fmt.Sprintf(":%d\r\n", v))
	case error:
		return []byte(fmt.Sprintf("-%s\r\n", v))
	case []string:
		return encodeStringArray(value.([]string))
	case [][]string:
		var b []byte
		buf := bytes.NewBuffer(b)
		for _, sa := range value.([][]string) {
			buf.Write(encodeStringArray(sa))
		}
		return []byte(fmt.Sprintf("*%d\r\n%s", len(value.([][]string)), buf.Bytes()))
	case []any:
		var b []byte
		buf := bytes.NewBuffer(b)
		for _, x := range value.([]any) {
			buf.Write(Encode(x, false))
		}
		return []byte(fmt.Sprintf("*%d\r\n%s", len(value.([]any)), buf.Bytes()))
	case []int:
		var b []byte
		buf := bytes.NewBuffer(b)
		for _, n := range value.([]int) {
			buf.Write([]byte(fmt.Sprintf("%d|", n)))
		}
		return []byte(fmt.Sprintf("@%s", buf.Bytes()))
	case any:
		return []byte(fmt.Sprintf("+%s%s", v, CRLF))
	default:
		return RespNil
	}
}

// +OK\r\n => OK, 5
func readSimpleString(data []byte) (string, int, error) {
	pos := 1
	for data[pos] != '\r' {
		pos++
	}
	return string(data[1:pos]), pos + 2, nil
}

// :123\r\n => 123
func readInt64(data []byte) (int64, int, error) {
	var res int64
	pos := 1
	neg := false
	if pos < len(data) && data[pos] == '-' {
		neg = true
		pos++
	}
	for pos < len(data) && data[pos] != '\r' {
		res = res*10 + int64(data[pos]-'0')
		pos++
	}
	if neg {
		res = -res
	}
	return res, pos + 2, nil
}

func readError(data []byte) (any, int, error) {
	msg, pos, err := readSimpleString(data)
	if err != nil {
		return nil, 0, err
	}
	return errors.New(msg), pos, nil
}

// $5\r\nhello\r\n => 5, 4
func readLen(data []byte) (int, int) {
	res, pos, _ := readInt64(data)
	return int(res), pos
}

// $5\r\nhello\r\n => "hello", $-1\r\n => ""
func readBulkString(data []byte) (string, int, error) {
	length, pos := readLen(data)
	if length < 0 {
		return "", pos, nil // null bulk string
	}
	end := pos + length
	if end > len(data) {
		return "", 0, fmt.Errorf("bulk string truncated: need %d bytes, have %d", length, len(data)-pos)
	}
	return string(data[pos:end]), end + 2, nil
}

// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n => {"hello", "world"}
func readArray(data []byte) (any, int, error) {
	length, pos := readLen(data)
	var res []any = make([]any, length)

	for i := range res {
		elem, delta, err := DecodeOne(data[pos:])
		if err != nil {
			return nil, 0, err
		}
		res[i] = elem
		pos += delta
	}
	return res, pos, nil
}

// @1|2|3 -> {1, 2, 3}
func readIntArray(data []byte) (any, int, error) {
	var res []int
	var pos int
	cur := 0
	for pos = 1; pos < len(data); pos++ {
		if data[pos] == '|' {
			res = append(res, cur)
			cur = 0
			continue
		}
		cur = cur*10 + int(data[pos]-'0')
	}
	return res, pos, nil
}

const GeoAlphabet string = "0123456789bcdefghjkmnpqrstuvwxyz"

type encoding struct {
	encode string
	decode [256]byte
}

func (e *encoding) Decode(s string) uint64 {
	var x uint64

	decode := [255]byte{}
	for i := 0; i < len(GeoAlphabet); i++ {
		decode[GeoAlphabet[i]] = byte(i)
	}
	for i := 0; i < 10; i++ {
		x = (x << 5) | uint64(decode[s[i]])
	}
	return x
}

/*
break x into 5-bit blocks and map each block to a character in GeoAlphabet.
If x is 52-bit long, the 2 last bits are encoded as 0. Example:

	  0b10010 11010 10010 10110 10100 10101 10101 00101 01101 01001 01
		    v     u     q     q     q     p     p     5     e     9  0
*/
func (e *encoding) Encode(x uint64) string {
	b := [11]byte{}
	for i := 0; i < 11; i++ {
		shift := 52 - (i+1)*5
		if shift <= 0 {
			b[i] = GeoAlphabet[0]
			break
		}
		idx := (x >> shift) & 0b11111
		b[i] = GeoAlphabet[idx]
	}
	return string(b[:])
}

func newBase32Encoding() *encoding {
	e := &encoding{
		encode: GeoAlphabet,
		decode: [256]byte{},
	}

	for i := 0; i < len(e.decode); i++ {
		e.decode[i] = 0xff
	}
	for i := 0; i < len(e.encode); i++ {
		e.decode[e.encode[i]] = byte(i)
	}
	return e
}
