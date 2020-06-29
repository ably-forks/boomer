package boomer

import (
	"encoding"
	"encoding/binary"
)

const (
	code_map32 byte = 0xdf
	code_int64 byte = 0xd3
)

func int32AsBytes(x uint32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, x)
	return result
}

func int64AsBytes(x int64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(x))
	return result
}

type IntMap map[int64]int64

func (m IntMap) MarshalBinary() (data []byte, err error) {
	elementCount := len(m)
	encodedLength := 5 + 18 * elementCount
	result := make([]byte, 0, encodedLength)

	// encode map 32
	result = append(result, code_map32)
	result = append(result, int32AsBytes(uint32(elementCount))...)

	// encode each element
	for k, v := range m {
		result = append(result, code_int64)
		result = append(result, int64AsBytes(k)...)
		result = append(result, code_int64)
		result = append(result, int64AsBytes(v)...)
	}
	return result, nil
}

var _ encoding.BinaryMarshaler = IntMap{}
