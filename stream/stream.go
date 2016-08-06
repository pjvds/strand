package stream

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pjvds/strand/message"
)

type Stream interface {
	Write(messages message.UnalignedSet) (message.Offset, error)
}

type stream struct {
	file *os.File

	offset   message.Offset
	position int64

	writeLock sync.Mutex
}

func NewStream(filename string) (Stream, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &stream{
		file:     file,
		offset:   message.EmptyOffset,
		position: 0,
	}, nil
}

func (this *stream) Write(messages message.UnalignedSet) (message.Offset, error) {
	this.writeLock.Lock()
	defer this.writeLock.Unlock()

	aligned := messages.Align(this.offset)
	buffer := aligned.GetBuffer()

	// TODO: cover too lesser writes
	written, err := this.file.WriteAt(buffer, this.position)
	if err != nil {
		return message.EmptyOffset, err
	}

	return this.advanceHead(written, aligned.DeltaOffset()), nil
}

func (this *stream) advanceHead(written int, offsetDelta message.Offset) message.Offset {
	atomic.AddInt64(&this.position, int64(written))
	newOffset := this.offset.Add(offsetDelta)
	this.offset = newOffset

	return newOffset
}

type Creator func(id Id) (Stream, error)

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
