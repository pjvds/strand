package message

import "fmt"

type UnalignedSet struct {
	index  []setIndex
	buffer []byte
}

type setIndex struct {
	position int
	size     int
	offset   Offset
}

func NewUnalignedSet(buffer []byte) (UnalignedSet, error) {
	position := 0
	index := make([]setIndex, 0, 8)

	for position < len(buffer) {
		// make sure there are enough bytes left for an int32
		if position+4 > len(buffer) {
			return UnalignedSet{}, fmt.Errorf("invalid message size at %v", position)
		}
		size := int(byteOrder.Uint32(buffer[position:]))

		if position+MESSAGE_SIZE_SIZE+size > len(buffer) {
			return UnalignedSet{}, fmt.Errorf("message too short at %v", position)
		}

		index = append(index, setIndex{
			position: position,
			size:     size,
		})

		position += MESSAGE_SIZE_SIZE + size
	}

	return UnalignedSet{
		index:  index,
		buffer: buffer,
	}, nil
}

func (this UnalignedSet) MessageCount() int {
	return len(this.index)
}

func (this UnalignedSet) Align(position Offset) AlignedSet {
	index := this.index
	buffer := this.buffer

	delta := Offset(this.MessageCount())

	for i := 0; i < len(index); i++ {
		index[i].offset = position.AddInt(i)
		setOffset(buffer, index[i], Offset(position.AddInt(i)))
	}

	// TODO: align messages
	return AlignedSet{
		index:  this.index,
		Buffer: this.buffer,

		FirstOffset: position,
		DeltaOffset: delta,
		LastOffset:  position.Add(delta),
	}
}

type AlignedSet struct {
	index  []setIndex
	Buffer []byte

	FirstOffset Offset
	DeltaOffset Offset
	LastOffset  Offset
}
