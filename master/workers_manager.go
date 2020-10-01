package master

import (
	"fmt"
	"sync"
	"time"

	"github.com/giulioborghesi/mapreduce/utils"

	"github.com/giulioborghesi/mapreduce/workers"
)

const (
	statusDeadlineInMs = 200
)

// workersManager keeps track of the workers health
type workersManager struct {
	wrkrs     map[int32]*worker
	activeCnt int
	cv        *sync.Cond
	sync.Mutex
}

// makeWorkersManager creates a new workersManager object from a list of
// workers
func makeWorkersManager(wrkrs []worker) *workersManager {
	m := new(workersManager)
	m.activeCnt = len(wrkrs)
	m.wrkrs = make(map[int32]*worker)
	for i := range wrkrs {
		wrkr := wrkrs[i]
		if _, ok := m.wrkrs[wrkr.id]; ok {
			panic(fmt.Sprintf("makeworkersmanager: worker %d already "+
				"registered", wrkr.id))
		}
		m.wrkrs[wrkr.id] = &wrkr
	}
	return m
}

// activeWorkers returns the number of active workers
func (m *workersManager) activeWorkers() int {
	return m.activeCnt
}

// reportFailedWorker should be used by clients to report failed workers. This
// method will panic if the specified worker ID is invalid
func (m *workersManager) reportFailedWorker(id int32) {
	if _, ok := m.wrkrs[id]; !ok {
		panic(fmt.Sprintf("reportfailedworker: invalid worker id: %d", id))
	}
	m.Lock()
	defer m.Unlock()

	if m.wrkrs[id].status == healthy {
		m.activeCnt--
	}
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
			client, err := utils.DialHTTP("tcp", wrkr.addr,
				statusDeadlineInMs*time.Millisecond)
			if err != nil {
				rchn <- dead
				return
			}

			err = client.Call(statusTask, workers.Void{}, new(workers.Void))
			client.Close()
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

	// Update workers status if needed
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

	// Update number of active workers and return
	cnt := 0
	for _, status := range res {
		if status == healthy {
			cnt++
		}
	}
	m.activeCnt = cnt
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
