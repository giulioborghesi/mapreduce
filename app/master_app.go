package app

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/giulioborghesi/mapreduce/service"
)

// StartMaster starts a MapReduce master
func StartMaster() {
	client, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		log.Fatal("master: dial http: ", err)
	}

	// Call Map function
	mapCtx := &service.MapRequestContext{TaskID: 0, FilePath: "/Users/giulioborghesi/tmp/example.dat"}
	reply := new(service.Status)

	err = client.Call("MapReduce.Map", mapCtx, reply)
	if err != nil {
		log.Fatal("master: server status: ", err)
	}

	// Call Reduce function
	reduceCtx := &service.ReduceRequestContext{FilePaths: []string{"/Users/giulioborghesi/tmp/0"}}

	err = client.Call("MapReduce.Reduce", reduceCtx, reply)
	if err != nil {
		log.Fatal("master: server status: ", err)
	}

	fmt.Printf("Server status: %v\n", bool(*reply))
}
