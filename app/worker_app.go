package app

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/giulioborghesi/mapreduce/utils"
	"github.com/giulioborghesi/mapreduce/workers"
)

// StartWorker starts a MapReduce RPC worker
func StartWorker(addr string) {
	// Extract port number from address string
	port, err := utils.GetPort(addr)
	if err != nil {
		panic(err)
	}

	// Register MapReduce service endpoints
	service := &workers.MapReduceService{}
	rpc.Register(service)
	rpc.HandleHTTP()

	// Register HTTP endpoint for data transfer
	//	http.HandleFunc("/data/", service.SendData)

	// Create listener and serve incoming requests
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("startworker:", err)
	}
	http.Serve(l, nil)
}
