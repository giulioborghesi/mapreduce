package main

import (
	"flag"
	"strings"

	"github.com/giulioborghesi/mapreduce/app"
)

func main() {
	// Parse arguments
	wrkrPtr := flag.String("workers", "localhost:1234", "Worker/workers address")
	rCntPtr := flag.Int("reducer_tasks", 1, "Number of reducer tasks")
	flag.Parse()

	// Unroll worker addresses
	addrs := strings.Split(*wrkrPtr, ",")

	// Start the master instance
	app.StartMaster(addrs, "/Users/giulioborghesi/tmp/example.dat", *rCntPtr)
}
