package message

import "fmt"

type UnalignedMessages struct {
	index  []messageIndex
	buffer []byte
}

type messageIndex struct {
	position int
	size     int
	offset   Offset
}

func NewUnalignedMessageSet(buffer []byte) (UnalignedMessages, error) {
	position := 0
	index := make([]messageIndex, 0, 8)

	for position < len(buffer) {
		// make sure there are enough bytes left for an int32
		if position+4 > len(buffer) {
			return UnalignedMessages{}, fmt.Errorf("invalid message size at %v", position)
		}
		size := int(byteOrder.Uint32(buffer[position:]))

		if position+MESSAGE_SIZE_SIZE+size > len(buffer) {
			return UnalignedMessages{}, fmt.Errorf("message too short at %v", position)
		}

		index = append(index, messageIndex{
			position: position,
			size:     size,
		})

		position += MESSAGE_SIZE_SIZE + size
	}

	return UnalignedMessages{
		index:  index,
		buffer: buffer,
	}, nil
}

func (this UnalignedMessages) MessageCount() int {
	return len(this.index)
}

func (this UnalignedMessages) Align(position Offset) AlignedMessages {
	index := this.index
	buffer := this.buffer

	delta := Offset(this.MessageCount())

	for i := 0; i < len(index); i++ {
		index[i].offset = position.AddInt(i)
		setOffset(buffer, index[i], Offset(position.AddInt(i)))
	}

	// TODO: align messages
	return AlignedMessages{
		index:  this.index,
		Buffer: this.buffer,

		FirstOffset: position,
		DeltaOffset: delta,
		LastOffset:  position.Add(delta),
	}
}

type AlignedMessages struct {
	index  []messageIndex
	Buffer []byte

	FirstOffset Offset
	DeltaOffset Offset
	LastOffset  Offset
}
