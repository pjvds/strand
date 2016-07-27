package main

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/pjvds/tidy"

	"github.com/pjvds/strand/api"
	"github.com/pjvds/strand/stream"
)

var log = tidy.Configure().
	LogFromLevel(tidy.DEBUG).To(tidy.Console).
	MustBuild()

type Server struct {
	streams *stream.Map
}

func NewServer(directory string) *Server {
	return &Server{
		streams: stream.NewMap(stream.Directory(directory).OpenOrCreateStream),
	}
}

func (this *Server) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendResponse, error) {
	id := stream.Id(request.Stream)
	s, err := this.streams.Get(id)

	if err != nil {
		return nil, err
	}

	set, err := stream.NewUnalignedMessageSet(request.Messages)
	if err != nil {
		return nil, err
	}

	_, err = s.Append(set)
	if err != nil {
		return nil, err
	}

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
	api.RegisterStrandServer(server, NewServer("/tmp"))

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
