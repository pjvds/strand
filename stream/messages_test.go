package stream

import("testing"
	"github.com/pjvds/randombytes"
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
)

func TestNewUnalignedMessageSet(t *testing.T) {
	assert := assert.New(t)

	set, err := NewUnalignedMessageSet(bufferWith5RandomMessages);

	assert.Nil(err)
	assert.Equal(5, set.MessageCount())
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

