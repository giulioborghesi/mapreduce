package main

import (
	"flag"
	"log"
	"strings"

	"github.com/giulioborghesi/mapreduce/app"
)

func main() {
	// Parse arguments
	rolePtr := flag.String("role", "master", "MapReduce role (master/worker)")
	wrkrPtr := flag.String("workers", "localhost:1234", "Worker/workers address")
	flag.Parse()

	// Unroll worker addresses
	addrs := strings.Split(*wrkrPtr, ",")

	// Start the right application based on the role
	switch *rolePtr {
	case "master":
		app.StartMaster(addrs, "giulio", 1, 1)
	case "worker":
		if len(addrs) != 1 {
			log.Fatal("main: cannot assign multiple addresses to same worker")
		}
		app.StartWorker(addrs[0])
	default:
		log.Fatal("main: invalid role: ", *rolePtr)
	}
}
