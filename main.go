package main

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/pjvds/tidy"

	"github.com/pjvds/strand/api"
	. "github.com/pjvds/strand/stream"
)

var log = tidy.Configure().
	LogFromLevel(tidy.DEBUG).To(tidy.Console).
	MustBuild()

type Server struct{}

func (this *Server) Append(context.Context, *api.AppendRequest) (*api.AppendResponse, error) {
	return &api.AppendResponse{
		Ok: true,
	}, nil
}
func (this *Server) Ping(context.Context, *api.PingRequest) (*api.PingResponse, error) {
	return &api.PingResponse{}, nil
}

func main() {

	network := "tcp"
	address := ":6300"
	listener, err := net.Listen(network, address)
	if err != nil {
		log.WithError(err).With("listen_address", address).Error("listen failure")
		return
	}

	server := grpc.NewServer()
	api.RegisterStrandServer(server, &Server{})

	log.Withs(tidy.Fields{
		"network": network,
		"address": address}).Info("listening")

	if err := server.Serve(listener); err != nil {
		log.WithError(err).Fatal("grpc server failed")
	}
}

type ReadError struct {
	Op  string
	Err error
}

func (this *ReadError) Error() string {
	return fmt.Sprintf("%v failed: %v", this.Op, this.Err.Error())
}

func Stopwatch(do func()) time.Duration {
	started := time.Now()
	do()
	return time.Since(started)
}

func handleConnection(conn net.Conn, streams *Map) error {
	// defer func() {
	// 	conn.Close()
	// 	if log.IsDebug() {
	// 		log.With("remote_address", conn.RemoteAddr()).Debug("connection closed")
	// 	}
	// }()
	// if log.IsDebug() {
	// 	log.With("remote_address", conn.RemoteAddr()).Debug("connection accepted")
	// }
	//
	// buffer := make([]byte, 8)
	// if _, err := ReadExact(conn, buffer); err != nil {
	// 	return &ReadError{
	// 		Op:  "read stream id lenght",
	// 		Err: err,
	// 	}
	// }
	// idSize := binary.LittleEndian.Uint32(buffer)
	// idSize := 4
	// buffer := make([]byte, idSize)
	//
	// if _, err := ReadExact(conn, buffer); err != nil {
	// 	return &ReadError{
	// 		Op:  "read stream id",
	// 		Err: err,
	// 	}
	// }
	//
	// id := Id(buffer)
	//
	// log.With("stream_id", id).Debug("stream id read")
	//
	// stream, err := streams.Get(Id(buffer))
	// if err != nil {
	// 	return err
	// }
	//
	// log.With("stream_id", id).Debug("stream retrieved")
	//
	// var copied int64
	//
	// elapsed := Stopwatch(func() {
	// 	var streamAsFile *os.File = stream
	// 	copied, err = io.Copy(streamAsFile, conn)
	// })
	//
	// mb := float64(copied) / 1e6
	// mbps := mb / elapsed.Seconds()
	//
	// log.Withs(tidy.Fields{
	// 	"mbps":    mbps,
	// 	"mb":      mb,
	// 	"bytes":   copied,
	// 	"elapsed": elapsed}).Info("connection done")
	//
	//return err
	return nil
}
