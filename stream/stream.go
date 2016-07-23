package stream

import (
	"os"
	"path/filepath"
	"sync"
)

type Stream *os.File

type StreamCreator func(id StreamId) (Stream, error)

type StreamMap struct {
	sync.RWMutex
	creator StreamCreator
	streams map[StreamId]Stream
}

func NewStreamMap(creator StreamCreator) *StreamMap {
	return &StreamMap{
		creator: creator,
		streams: make(map[StreamId]Stream),
	}
}

func (this *StreamMap) Get(id StreamId) (Stream, error) {
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

type StreamId string

type StreamDirectory string

func (this StreamDirectory) OpenOrCreateStream(id StreamId) (Stream, error) {
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
