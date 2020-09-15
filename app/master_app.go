package app

import (
	"log"
	"net/rpc"

	"github.com/giulioborghesi/mapreduce/service"
	"github.com/giulioborghesi/mapreduce/utils"
)

// StartMaster starts a MapReduce master
func StartMaster() {
	client, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		log.Fatal("master: dial http: ", err)
	}

	// Call Map function
	mapCtx := &service.MapRequestContext{TaskID: 0, FilePath: "/Users/giulioborghesi/tmp/example.dat"}
	reply := new(service.Void)

	err = client.Call("MapService.Map", mapCtx, reply)
	if err != nil {
		log.Fatal("master: server status: ", err)
	}

	// Call Reduce function
	sources := []utils.DataSource{utils.DataSource{File: "0", Host: "localhost"}}
	reduceCtx := &service.ReduceRequestContext{Sources: sources}

	err = client.Call("ReduceService.Reduce", reduceCtx, reply)
	if err != nil {
		log.Fatal("master: server status: ", err)
	}

}
