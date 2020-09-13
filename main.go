package main

import (
	"flag"
	"log"

	"github.com/giulioborghesi/mapreduce/app"
)

func main() {
	// Parse arguments
	rolePtr := flag.String("role", "master", "MapReduce role (master / worker)")
	flag.Parse()

	// Start the right application based on the role
	switch *rolePtr {
	case "master":
		app.StartMaster()
	case "worker":
		app.StartWorker()
	default:
		log.Fatal("main: invalid role: ", *rolePtr)
	}
}
