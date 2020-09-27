package app

import (
	"log"
	"net"
	"time"

	"github.com/giulioborghesi/mapreduce/master"
)

const (
	// retries is the maximum number of attempts to contact workers
	retries = 17
)

// findActiveWorkers attempts to contact candidate workers; on success, these
// workers will be added to the pool of available workers. The algorithm uses
// retries with exponential backoff to wait for workers whose initialization
// is slow
func findActiveWorkers(addrs []string) []string {
	fact := 1
	res := make([]string, 0, len(addrs))

	n := len(addrs)
	for i := 1; i <= retries; i++ {
		log.Println("Contacting workers, attempt: ", i)
		for j := len(addrs) - 1; j >= 0; j-- {
			conn, err := net.Dial("tcp", addrs[j])
			if err == nil {
				conn.Close()
				res = append(res, addrs[j])
				addrs = addrs[:n-1]
				n--
			}
		}

		if n == 0 || i == retries {
			break
		}

		wait := time.Duration(fact) * time.Millisecond
		log.Println("Not all workers available, waiting for ", wait,
			" before retrying")
		time.Sleep(wait)
		fact *= 2
	}

	return res
}

// StartMaster initializes the MapReduce master and starts the MapReduce
// computation
func StartMaster(addrs []string, filePath string, reducerCnt int) {
	addrs = findActiveWorkers(addrs)
	if len(addrs) == 0 {
		log.Println("No worker available, terminating program...")
		return
	}

	c := master.MakeCoordinator(addrs, filePath, 1, reducerCnt)
	c.Run()
}
