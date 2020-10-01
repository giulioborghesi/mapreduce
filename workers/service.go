package workers

import (
	"fmt"
	"sync"

	"github.com/giulioborghesi/mapreduce/common"
)

// Void is a dummy type used for empty RPC arguments
type Void struct{}

// MapReduceService implements a MapReduce RPC service
type MapReduceService struct {
	tsk2host map[string]map[int]common.Host
	mu       sync.Mutex
}

// MakeMapReduceService creates, initializes and return an instance of a
// MapReduce service
func MakeMapReduceService() *MapReduceService {
	srvc := new(MapReduceService)
	srvc.tsk2host = make(map[string]map[int]common.Host)
	return srvc
}

// host returns the host information for a mapper task with specified index
// that processed a specified file
func (srvc *MapReduceService) host(file string, idx int) common.Host {
	srvc.mu.Lock()
	defer srvc.mu.Unlock()

	if _, ok := srvc.tsk2host[file]; !ok {
		return common.Host("")
	}
	if _, ok := srvc.tsk2host[file][idx]; !ok {
		panic(fmt.Sprintf("host: invalid task idx: %d", idx))
	}
	return srvc.tsk2host[file][idx]
}
