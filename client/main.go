package main

import (
	"fmt"
	"log"
	"os"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/pjvds/stopwatch"
	"github.com/pjvds/strand/api"
	"github.com/urfave/cli"
)

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
			Usage:   "publish messages to topic",
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
					_, err = client.Append(context.Background(), &api.AppendRequest{
						Stream:   "client",
						Messages: make([]byte, 1024),
					})
				})

				if err != nil {
					fmt.Printf("request failed: %v", err)
				}

				fmt.Printf("elapsed: %v", elapsed)
				return nil
			},
		},
	}

	app.Run(os.Args)
}
