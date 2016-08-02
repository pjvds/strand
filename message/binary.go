package message

import "encoding/binary"

var byteOrder = binary.LittleEndian

const (
	MESSAGE_SIZE_SIZE = 4
	OFFSET_SIZE       = 8
)

func alterOffsetInSetBuffer(mesageSetBuffer []byte, index setIndex) {
	location := index.position + MESSAGE_SIZE_SIZE
	byteOrder.PutUint64(mesageSetBuffer[location:], uint64(index.offset))
}
