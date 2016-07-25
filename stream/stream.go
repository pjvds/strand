package stream

import (
	"os"
	"path/filepath"
	"sync"
)

type Stream *os.File

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

		return os.Create(path)
	}

	return os.Open(path)
}
