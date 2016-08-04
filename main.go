package main

import (
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/pjvds/tidy"

	"github.com/pjvds/strand/api"
	"github.com/pjvds/strand/server"
)

var log = tidy.Configure().
	LogFromLevel(tidy.DEBUG).To(tidy.Console).
	MustBuild()

func main() {
	network := "tcp"
	address := ":6300"
	listener, err := net.Listen(network, address)
	if err != nil {
		log.WithError(err).With("listen_address", address).Error("listen failure")
		return
	}

	grpcServer := grpc.NewServer()
	strandServer, _ := server.NewServer("/tmp")
	api.RegisterStrandServer(grpcServer, strandServer)

	log.Withs(tidy.Fields{
		"network": network,
		"address": address}).Info("listening")

	if err := grpcServer.Serve(listener); err != nil {
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
