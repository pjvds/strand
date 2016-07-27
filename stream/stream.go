package stream

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
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

	this.position += int64(written) + 1
	this.offset = aligned.LastOffset.Next()

	return aligned.LastOffset, nil
}

type Offset uint64

const EmptyOffset Offset = 0

func (this Offset) Next() Offset {
	return Offset(this + 1)
}

type Creator func(id Id) (Stream, error)

type UnalignedMessages struct {
	Buffer []byte
}

func NewUnalignedMessageSet(buffer []byte) (UnalignedMessages, error) {
	return UnalignedMessages{
		Buffer: buffer,
	}, nil
}

func (this UnalignedMessages) Align(position Offset) AlignedMessages {
	// TODO: align messages
	return AlignedMessages{
		Position: position,
		Buffer:   this.Buffer,
	}
}

type AlignedMessages struct {
	Position Offset
	Buffer   []byte

	LastOffset Offset
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
