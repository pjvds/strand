package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Set struct {
	index      []setIndex
	buffer     *bytes.Buffer
	lastOffset Offset
}

func (this *Set) Append(message []byte) {
	position := this.buffer.Len()

	size := MESSAGE_SIZE_SIZE + OFFSET_SIZE + len(message)
	offset := this.lastOffset.Next()

	// Write appends the given content to the buffer, growing the buffer as needed.
	// Err is always nil. If the buffer becomes too large, it will panic with ErrTooLarge.
	// Therefor we don't need to check written bytes or err.
	binary.Write(this.buffer, byteOrder, size)
	binary.Write(this.buffer, byteOrder, offset)
	this.buffer.Write(message)

	this.index = append(this.index, setIndex{
		position: position,
		size:     size,
		offset:   offset,
	})

	this.lastOffset = offset
}

func (this *Set) GetBuffer() []byte {
	return this.buffer.Bytes()
}

func (this *Set) MessageCount() int {
	return len(this.index)
}

func (this *Set) FirstOffset() Offset {
	if len(this.index) == 0 {
		return EmptyOffset
	}

	return this.index[0].offset
}

func (this *Set) DeltaOffset() Offset {
	return this.LastOffset().Sub(this.FirstOffset())
}

func (this *Set) LastOffset() Offset {
	if len(this.index) == 0 {
		return EmptyOffset
	}

	return this.index[len(this.index)-1].offset
}

type UnalignedSet struct {
	Set
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
		Set: Set{
			index:  index,
			buffer: bytes.NewBuffer(buffer),
		},
	}, nil
}

func (this UnalignedSet) Align(startOffset Offset) AlignedSet {
	index := this.index
	buffer := this.buffer

	// Bytes returns a slice of length b.Len() holding the unread portion of the buffer.
	// The slice is valid for use only until the next buffer modification (that is,
	// only until the next call to a method like Read, Write, Reset, or Truncate).
	// The slice aliases the buffer content at least until the next buffer modification,
	// immediate changes to the slice will affect the result of future reads.
	bytes := buffer.Bytes()

	for i := 0; i < len(index); i++ {
		offset := startOffset.AddInt(i)

		index[i].offset = offset
		alterOffsetInSetBuffer(bytes, index[i])
	}

	// TODO: align messages
	return AlignedSet{
		Set: Set{
			index:  index,
			buffer: buffer,
		},
	}
}

type AlignedSet struct{ Set }
