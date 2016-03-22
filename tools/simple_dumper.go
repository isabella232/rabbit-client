package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/codegangsta/cli"
	rc "github.com/netlify/rabbit-client"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Value: "amqps://rabbit.lo:5671",
			Usage: "which server to connect to",
		},
		cli.StringFlag{
			Name:  "cert, c",
			Value: "/usr/local/etc/certs/test.pem",
			Usage: "What public cert file to use",
		},
		cli.StringFlag{
			Name:  "key, k",
			Value: "/usr/local/etc/certs/test-key.pem",
			Usage: "What private cert file to use",
		},
		cli.StringFlag{
			Name:  "cacert, ca",
			Value: "/usr/local/etc/certs/ca.pem",
			Usage: "What ca public file to use",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "connect",
			Usage:  "connect [exchange]",
			Action: connectToBroker,
		},
		{
			Name:   "listen",
			Usage:  "listen <exchange> [queue]",
			Action: listenAndDump,
		},
	}
	app.Run(os.Args)
}

func connectToBroker(c *cli.Context) {
	url := fmt.Sprintf(c.GlobalString("url"))
	tlsConf := rc.TLSConfiguration{
		Cert:    c.GlobalString("cert"),
		Key:     c.GlobalString("key"),
		CACerts: []string{c.GlobalString("cacert")},
	}

	asBytes, _ := json.Marshal(tlsConf)
	fmt.Println("connecting to " + url + ": " + string(asBytes))

	_, err := rc.Dial(url, &tlsConf)
	if err != nil {
		panic(err)
	}
	log.Println("Connection to broker successful")

	if len(c.Args()) == 1 {
		// TODO connect to the Exchange
		panic("currently don't support connecting to an exchange")
	}
}

func listenAndDump(c *cli.Context) {
	panic("yeah we should do this")
}
