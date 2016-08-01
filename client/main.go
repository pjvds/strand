package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/pjvds/randombytes"
	"github.com/pjvds/stopwatch"
	"github.com/pjvds/strand/api"
	"github.com/urfave/cli"
)

type Session struct {
	client api.StrandClient

	ctx context.Context
}

func Dial(address string) (*Session, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Session{
		client: api.NewStrandClient(conn),
		ctx:    context.TODO(), // TODO: use cancelable context
	}, nil
}

func (this *Session) Append(stream string, message []byte) error {
	_, err := this.client.Append(ctx, &api.AppendRequest{
		Stream: stream,
	})

	return err
}

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		{
			Name:  "ping",
			Usage: "ping host",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "host",
					Value:  "localhost:6300",
					Usage:  "the address of the host",
					EnvVar: "STRAND_HOST",
				},
			},
			Action: func(c *cli.Context) error {
				host := c.String("host")

				// Set up a connection to the server.
				conn, err := grpc.Dial(host, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("failed to connect: %v", err)
				}
				defer conn.Close()

				client := api.NewStrandClient(conn)
				elapsed := stopwatch.Time(func() {
					_, err = client.Ping(context.Background(), &api.PingRequest{})
				})

				if err != nil {
					fmt.Printf("request failed: %v", err)
				}

				fmt.Printf("elapsed: %v", elapsed)
				return nil
			},
		},
		{
			Name:    "append",
			Aliases: []string{"a"},
			Usage:   "append messages to topic",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "host",
					Value:  "localhost:6300",
					Usage:  "the address of the host",
					EnvVar: "STRAND_HOST",
				},
			},
			Action: func(c *cli.Context) error {
				host := c.String("host")

				// Set up a connection to the server.
				conn, err := grpc.Dial(host, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("failed to connect: %v", err)
				}
				defer conn.Close()

				client := api.NewStrandClient(conn)

				var work sync.WaitGroup
				done := make(chan struct{})

				var bytesSend int64
				messages := randombytes.Make(8096)

				watch := stopwatch.Start()
				for i := 0; i < 16; i++ {
					work.Add(1)

					streamId := fmt.Sprintf("client%v", i)

					go func(streamId string) {
						defer work.Done()

						for {
							select {
							case <-done:
								return
							default:
								_, err = client.Append(context.Background(), &api.AppendRequest{
									Stream:   streamId,
									Messages: messages,
								})

								if err != nil {
									fmt.Printf("append failed: %v\n", err)
								}

								atomic.AddInt64(&bytesSend, 8096)
							}
						}
					}(streamId)
				}

				<-time.After(time.Minute)
				elapsed := watch.Elapsed()

				close(done)

				work.Wait()

				fmt.Printf("elapsed: %v, bytes_sent: %v", elapsed, bytesSend)
				fmt.Printf("mbps: %v", (float64(bytesSend)/1e6)/elapsed.Seconds())
				return nil
			},
		},
	}

	app.Run(os.Args)
}
