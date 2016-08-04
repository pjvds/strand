package server

import (
	"github.com/pjvds/strand/api"
	"github.com/pjvds/strand/message"
	"github.com/pjvds/strand/stream"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
)

var log = tidy.Configure().
	LogFromLevel(tidy.DEBUG).To(tidy.Console).
	MustBuild()

type Server struct {
	streams *stream.Map
}

func NewServer(directory string) (*Server, error) {
	streamDir := stream.Directory(directory)

	return &Server{
		streams: stream.NewMap(streamDir.OpenOrCreateStream),
	}, nil
}

func (this *Server) Write(ctx context.Context, request *api.WriteRequest) (*api.WriteResponse, error) {
	id := stream.Id(request.Stream)
	if log.IsDebug() {
		log.With("stream_id", id).Debug("handling append request")
	}

	s, err := this.streams.Get(id)
	if err != nil {
		if log.IsInfo() {
			log.With("stream_id", id).WithError(err).Info("failed to get stream")
		}
		return nil, err
	}

	set, err := message.NewUnalignedSet(request.Messages)
	if err != nil {
		if log.IsDebug() {
			log.WithError(err).Debug("failed to parse message set")
		}
		return nil, err
	}

	_, err = s.Append(set)
	if err != nil {
		return nil, err
	}

	return &api.WriteResponse{
		Ok: true,
	}, nil
}

func (this *Server) Ping(context.Context, *api.PingRequest) (*api.PingResponse, error) {
	return &api.PingResponse{}, nil
}
