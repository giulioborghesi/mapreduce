package app

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/giulioborghesi/mapreduce/service"
)

// StartWorker starts a MapReduce RPC worker
func StartWorker() {
	// Register MapReduce service
	s := &service.MapReduce{}
	rpc.Register(s)
	rpc.HandleHTTP()

	// Create listener and serve incoming requests
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	http.Serve(l, nil)
}
