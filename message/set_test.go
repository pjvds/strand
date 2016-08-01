package message

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/pjvds/randombytes"
	"github.com/stretchr/testify/assert"
)

func TestNewUnalignedMessageSet(t *testing.T) {
	assert := assert.New(t)

	set, err := NewUnalignedSet(bufferWith5RandomMessages)

	assert.Nil(err)
	assert.Equal(5, set.MessageCount())
}

func TestUnalignedMessages_Align(t *testing.T) {
	assert := assert.New(t)
	unalignedSet, _ := NewUnalignedSet(bufferWith5RandomMessages)

	set := unalignedSet.Align(Offset(12))

	assert.Equal(Offset(12), set.FirstOffset, "first offset")
	assert.Equal(Offset(5), set.DeltaOffset, "delta offset")
	assert.Equal(Offset(17), set.LastOffset, "last offset")

	for i := 0; i < len(set.index); i++ {
		assert.Equal(Offset(12+i), set.index[i].offset, "index offset at %v", i)
	}
}

var bufferWith5RandomMessages = func() []byte {
	buffer := new(bytes.Buffer)

	for i := 0; i < 5; i++ {
		size := i * 50
		message := randombytes.Make(size)

		binary.Write(buffer, byteOrder, int32(size))
		buffer.Write(message)
	}

	return buffer.Bytes()
}()
