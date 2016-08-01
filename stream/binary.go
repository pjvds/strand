package stream

import "encoding/binary"

var byteOrder = binary.LittleEndian

const(
	MESSAGE_SIZE_SIZE = 4
)

func setOffset(mesageSetBuffer []byte, index messageIndex, offset Offset) {
	location := index.position + MESSAGE_SIZE_SIZE
	byteOrder.PutUint64(mesageSetBuffer[location:], uint64(offset))
}