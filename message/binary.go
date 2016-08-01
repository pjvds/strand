package message

import "encoding/binary"

var byteOrder = binary.LittleEndian

const (
	MESSAGE_SIZE_SIZE = 4
)

func alterOffsetInSetBuffer(mesageSetBuffer []byte, index setIndex, offset Offset) {
	location := index.position + MESSAGE_SIZE_SIZE
	byteOrder.PutUint64(mesageSetBuffer[location:], uint64(offset))
}
