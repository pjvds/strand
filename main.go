package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pjvds/tidy"
)

var log = tidy.Configure().
	LogFromLevel(tidy.DEBUG).To(tidy.Console).
	MustBuild()

type Stream *os.File

type AppendRequest struct {
	Id       StreamId
	Messages UnalignedMessages
}

type UnalignedMessages []byte

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

func (this *StreamMap) get(id StreamId) (Stream, error) {
	stream, ok := func() (Stream, bool) {
		this.RLock()
		defer this.RUnlock()

		stream, ok := this.streams[id]
		return stream, ok
	}()

	if ok {
		return stream, nil
	}

	this.Lock()
	defer this.Unlock()

	stream, ok = this.streams[id]

	if ok {
		return stream, nil
	}

	// we don't have the stream, create it
	created, err := this.creator(id)

	if err != nil {
		return nil, err
	}

	this.streams[id] = created
	return created, nil
}

type StreamId string

type StreamDirectory string

func (this StreamDirectory) getOrCreateStream(id StreamId) (Stream, error) {
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

func main() {
	log.Info("starting")

	network := "tcp"
	address := ":5000"
	listener, err := net.Listen(network, address)

	streamDirectory := StreamDirectory("/tmp/")
	streams := NewStreamMap(streamDirectory.getOrCreateStream)

	if err != nil {
		log.WithError(err).With("listen_address", address).Error("listen failure")
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			// handle error
			log.WithError(err).Error("accept failure")
			break
		}

		go handleConnection(conn, streams)
	}
}

type ReadError struct {
	Op  string
	Err error
}

func (this *ReadError) Error() string {
	return fmt.Sprintf("%v failed: %v", this.Op, this.Err.Error())
}

func ReadExact(reader io.Reader, buffer []byte) (int, error) {
	n, err := reader.Read(buffer)

	if err != nil {
		return n, err
	} else if n < len(buffer) {
		err = errors.New("short read")
	}

	return n, err
}

func Stopwatch(do func()) time.Duration {
	started := time.Now()
	do()
	return time.Since(started)
}

func handleConnection(conn net.Conn, streams *StreamMap) error {
	defer func() {
		conn.Close()
		if log.IsDebug() {
			log.With("remote_address", conn.RemoteAddr()).Debug("connection closed")
		}
	}()
	if log.IsDebug() {
		log.With("remote_address", conn.RemoteAddr()).Debug("connection accepted")
	}

	// buffer := make([]byte, 8)
	// if _, err := ReadExact(conn, buffer); err != nil {
	// 	return &ReadError{
	// 		Op:  "read stream id lenght",
	// 		Err: err,
	// 	}
	// }
	// idSize := binary.LittleEndian.Uint32(buffer)
	idSize := 4
	buffer := make([]byte, idSize)

	if _, err := ReadExact(conn, buffer); err != nil {
		return &ReadError{
			Op:  "read stream id",
			Err: err,
		}
	}

	id := StreamId(buffer)

	log.With("stream_id", id).Debug("stream id read")

	stream, err := streams.get(StreamId(buffer))
	if err != nil {
		return err
	}

	log.With("stream_id", id).Debug("stream retrieved")

	var copied int64

	elapsed := Stopwatch(func() {
		var streamAsFile *os.File = stream
		copied, err = io.Copy(streamAsFile, conn)
	})

	mb := float64(copied) / 1e6
	mbps := mb / elapsed.Seconds()

	log.With("mbps", mbps).Info("connection done")

	return err
}
