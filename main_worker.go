package main

import (
	"flag"

	"github.com/giulioborghesi/mapreduce/app"
)

func main() {
	// Parse argument
	addrPtr := flag.String("address", "localhost:1234", "Worker address")
	flag.Parse()

	// Start a worker instance
	app.StartWorker(*addrPtr)
}
