package master

import (
	"fmt"
	"sync"

	"github.com/giulioborghesi/mapreduce/workers"
)

type tasksManager struct {
	tsks     map[int32]*task
	wrkr2tsk map[int32]map[int32]bool
	tskLeft  int
	sync.Mutex
}

// makeTasksManager creates a new tasksManager object from a slice of tasks.
// The tasks in the slice are required to have distinct IDs, otherwise the
// function will panic
func makeTasksManager(tsks []task) *tasksManager {
	m := new(tasksManager)
	m.tsks = make(map[int32]*task)
	m.wrkr2tsk = make(map[int32]map[int32]bool)

	reduceTskCnt := 0
	for idx := range tsks {
		tsk := tsks[idx]
		if _, ok := m.tsks[tsk.id]; ok {
			panic(fmt.Sprintf("maketasksmonitor: task %d already registered",
				tsk.id))
		}
		m.tsks[tsk.id] = &tsk
		if tsk.method == reduceTask {
			reduceTskCnt++
		}
	}
	m.tskLeft = reduceTskCnt
	return m
}

// assignWorkerToTask assigns a worker to a task. This function will change the
// task status to in progress and update the list of tasks assigned to the
// worker. The task must be valid, otherwise the function will panic
func (m *tasksManager) assignWorkerToTask(wrkrID, tskID int32) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.wrkr2tsk[wrkrID]; !ok {
		m.wrkr2tsk[wrkrID] = make(map[int32]bool)
	}
	m.wrkr2tsk[wrkrID][tskID] = true

	ts, ok := m.tsks[tskID]
	if !ok {
		panic(fmt.Sprintf("assignworkertotask: task %d not found", tskID))
	}

	if ts.status != idle {
		panic(fmt.Sprintf("assignworkertotask: task %d already assigned to "+
			"worker", tskID))
	}
	m.tsks[tskID].wrkrID = wrkrID
	m.tsks[tskID].status = inProgress
}

// reduceTasksLeft returns the number of reduce tasks left to complete the
// MapReduce computation
func (m *tasksManager) reduceTasksLeft() int {
	return m.tskLeft
}

// task returns a pointer to the task with the specified id. This method will
// panic if no task with the specified id exists
func (m *tasksManager) task(tskID int32) *task {
	if _, ok := m.tsks[tskID]; !ok {
		panic(fmt.Sprintf("task: task %d not found", tskID))
	}
	return m.tsks[tskID]
}

// updatedTasksStatus updates the tasks status based on the workersstatus and
// returns a dictionary of tasks status indexed by the task id
func (m *tasksManager) updatedTasksStatus(
	wrkrs map[int32]workerStatus) map[int32]taskStatus {
	m.Lock()
	defer m.Unlock()

	// If worker failed, set status of dependent tasks to failed
	for wrkrID, wrkrStatus := range wrkrs {
		if _, ok := m.wrkr2tsk[wrkrID]; !ok {
			continue
		}

		if wrkrStatus != healthy {
			for tskID := range m.wrkr2tsk[wrkrID] {
				m.tsks[tskID].wrkrID = invalidWorkerID
				m.tsks[tskID].status = failed
			}
		}
	}

	// Reset failed tasks status to idle and append to return slice
	res := make(map[int32]taskStatus, 0)
	m.tskLeft = 0
	for tskID, tsk := range m.tsks {
		res[tskID] = tsk.status
		if tsk.status == failed {
			tsk.status = idle
		}

		if tsk.method == reduceTask && tsk.status != done {
			m.tskLeft++
		}
	}
	return res
}

// updateTaskStatus updates the status of a task associated with a worker. Both
// the task and the worker must be valid; additionally, the task must be
// associated with the worker. If these conditions are not satisfied, this
// method will panic
func (m *tasksManager) updateTaskStatus(tskStatus workers.Status,
	tskID int32) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.tsks[tskID]; !ok {
		panic(fmt.Sprintf("updatetaskstatus: task %d not found", tskID))
	}

	if tskStatus == workers.SUCCESS {
		m.tsks[tskID].status = done
		if m.tsks[tskID].method == reduceTask {
			m.tskLeft--
		}
	} else {
		wrkrID := m.tsks[tskID].wrkrID
		delete(m.wrkr2tsk[wrkrID], tskID)
		m.tsks[tskID].wrkrID = invalidWorkerID
		m.tsks[tskID].status = failed
	}
}
