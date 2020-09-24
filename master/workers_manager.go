package master

import (
	"fmt"
	"net/rpc"
	"sync"

	"github.com/giulioborghesi/mapreduce/service"
)

// workersManager keeps track of the workers health
type workersManager struct {
	wrkrs map[int32]*worker
	sync.Mutex
}

// makeWorkersManager creates a new workersManager object from a list of
// workers' addresses
func makeWorkersManager(addrs []string) *workersManager {
	m := new(workersManager)
	for i, addr := range addrs {
		id := int32(i)
		m.wrkrs[id] = &worker{id: id, addr: addr, status: healthy}
	}
	return m
}

// reportFailedWorker should be used by clients to report failed workers. This
// method will panic if the specified worker ID is invalid
func (m *workersManager) reportFailedWorker(id int32) {
	if _, ok := m.wrkrs[id]; !ok {
		panic(fmt.Sprintf("reportfailedworker: invalid worker id: %d", id))
	}
	m.Lock()
	defer m.Unlock()
	m.wrkrs[id].status = dead
}

// updatedWorkersStatus updates the status of each worker and returns a map
// from worker ID to worker status
func (m *workersManager) updatedWorkersStatus() map[int32]workerStatus {
	chans := make(map[int32]chan workerStatus)
	for id := range m.wrkrs {
		wrkr := m.wrkrs[id]
		if wrkr.status == dead {
			continue
		}

		rchn := make(chan workerStatus)
		chans[id] = rchn
		go func() {
			client, err := rpc.DialHTTP("tcp", wrkr.addr)
			if err != nil {
				rchn <- dead
				return
			}

			err = client.Call(statusTask, service.Void{}, new(service.Void))
			if err != nil {
				rchn <- dead
				return
			}

			rchn <- healthy
		}()
	}

	// Get workers status
	newStatus := make(map[int32]workerStatus)
	for id, rchn := range chans {
		newStatus[id] = <-rchn
	}
	res := make(map[int32]workerStatus)

	m.Lock()
	defer m.Unlock()

	// Update workers status if needed and return
	for id := range m.wrkrs {
		res[id] = m.wrkrs[id].status
		if _, ok := newStatus[id]; !ok {
			continue
		}

		if status := newStatus[id]; status == dead {
			res[id] = dead
			m.wrkrs[id].status = dead
		}
	}
	return res
}

// worker returns the worker information for the worker with the specified ID.
// This method will panic if the specified worker ID is invalid
func (m *workersManager) worker(id int32) *worker {
	if _, ok := m.wrkrs[id]; !ok {
		panic(fmt.Sprintf("worker: invalid worker id: %d", id))
	}
	return m.wrkrs[id]
}
