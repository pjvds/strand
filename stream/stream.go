package stream

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"fmt"
)

type Stream interface {
	Append(messages UnalignedMessages) (Offset, error)
}

type stream struct {
	file *os.File

	offset   Offset
	position int64
}

func NewStream(filename string) (Stream, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &stream{
		file:     file,
		offset:   EmptyOffset,
		position: 0,
	}, nil
}

func (this *stream) Append(messages UnalignedMessages) (Offset, error) {
	aligned := messages.Align(this.offset)
	buffer := aligned.Buffer

	// TODO: cover too lesser writes
	written, err := this.file.WriteAt(buffer, this.position)
	if err != nil {
		return EmptyOffset, err
	}

	return this.advanceHead(written, aligned.DeltaOffset), nil
}

func (this *stream) advanceHead(written int, offsetDelta Offset) Offset {
	atomic.AddInt64(&this.position, int64(written))
	newOffset := this.offset.Add(offsetDelta)
	this.offset = newOffset

	return newOffset
}

type Offset uint64

func (this Offset) GoString() string {
	return fmt.Sprint(this)
}

const EmptyOffset Offset = 0

func (this Offset) Next() Offset {
	return Offset(this + 1)
}

func (this Offset) Add(delta Offset) Offset {
	return Offset(this + delta)
}

func (this Offset) AddInt(delta int) Offset {
	return Offset(uint64(this) + uint64(delta))
}

type Creator func(id Id) (Stream, error)

type messageIndex struct{
	position int
	size int
	offset Offset
}

type UnalignedMessages struct {
	index []messageIndex
	buffer []byte
}

func NewUnalignedMessageSet(buffer []byte) (UnalignedMessages, error) {
	position := 0
	index := make([]messageIndex, 0, 8)

	for position < len(buffer) {
		// make sure there are enough bytes left for an int32
		if position + 4 > len(buffer) {
			return UnalignedMessages{}, fmt.Errorf("invalid message size at %v", position)
		}
		size := int(byteOrder.Uint32(buffer[position:]))

		if position + MESSAGE_SIZE_SIZE + size > len(buffer) {
			return UnalignedMessages{}, fmt.Errorf("message too short at %v", position)
		}

		index = append(index, messageIndex{
			position: position,
			size: size,
		})

		position += MESSAGE_SIZE_SIZE + size
	}


	return UnalignedMessages{
		index: index,
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
		index: this.index,
		Buffer:   this.buffer,

		FirstOffset: position,
		DeltaOffset: delta,
		LastOffset:  position.Add(delta),
	}
}

type AlignedMessages struct {
	index []messageIndex
	Buffer   []byte

	FirstOffset Offset
	DeltaOffset Offset
	LastOffset  Offset
}

type Map struct {
	sync.RWMutex
	creator Creator
	streams map[Id]Stream
}

func NewMap(creator Creator) *Map {
	return &Map{
		creator: creator,
		streams: make(map[Id]Stream),
	}
}

func (this *Map) Get(id Id) (Stream, error) {
	// try to get the stream from memory
	if stream, ok := func() (Stream, bool) {
		this.RLock()
		defer this.RUnlock()

		stream, ok := this.streams[id]
		return stream, ok
	}(); ok {
		// got it
		return stream, nil
	}

	// we don't have the stream in memory,
	// acquire write lock so we can try
	// to add it
	this.Lock()
	defer this.Unlock()

	// it might be that another routine
	// acquired the lock before us and
	// added the stream to memory
	if stream, ok := this.streams[id]; ok {
		return stream, nil
	}

	created, err := this.creator(id)
	if err != nil {
		return nil, err
	}

	this.streams[id] = created
	return created, nil
}

type Id string

type Directory string

func (this Directory) OpenOrCreateStream(id Id) (Stream, error) {
	directory := string(this)
	filename := string(id) + ".str"
	path := filepath.Join(directory, filename)

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		return NewStream(path)
	}

	return nil, errors.New("open existing stream is still unsupported")
}
