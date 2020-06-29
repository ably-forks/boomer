package boomer

import (
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
	"testing"
)

func Test_Encoding(t *testing.T) {
	mh := &codec.MsgpackHandle{}

	v := IntMap(map[int64]int64{
		1: 10,
		2: 1000000000,
		-2: -4,
	})

	// encode
	var out []byte
	enc := codec.NewEncoderBytes(&out, mh)
	err := enc.Encode(v)
	assert.NoError(t, err)

	// decode
	dec := codec.NewDecoderBytes(out, mh)
	decoded := &IntMap{}
	err = dec.Decode(decoded)
	assert.NoError(t, err)

	// check
	assert.Equal(t, v, *decoded)
}
