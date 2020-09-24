package master

import (
	"fmt"
	"sync"
)

type tasksManager struct {
	tsks     map[int32]*task
	wrkr2tsk map[int32]map[int32]bool
	sync.Mutex
}

// makeTasksManager creates a new tasksManager object from a slice of tasks.
// The tasks in the slice are required to have distinct IDs, otherwise the
// function will panic
func makeTasksManager(ts []task) *tasksManager {
	m := new(tasksManager)
	for _, t := range ts {
		if _, ok := m.tsks[t.id]; ok {
			panic(fmt.Sprintf("maketasksmonitor: task already registered"))
		}
		m.tsks[t.id] = &t
	}
	return m
}

// assignWorkerToTask assigns a worker to a task. This function will change the
// task status to in progress and update the list of tasks assigned to the
// worker. The task must be valid, otherwise the function will panic
func (m *tasksManager) assignWorkerToTask(wrkrID, tskID int32) {
	m.Lock()
	defer m.Unlock()

	m.wrkr2tsk[wrkrID][tskID] = true

	ts, ok := m.tsks[tskID]
	if !ok {
		panic(fmt.Sprintf("assignworkertotask: task not found"))
	}

	if ts.status != idle {
		panic(fmt.Sprintf("assignworkertotask: task already assigned to worker"))
	}
	m.tsks[tskID].status = inProgress
}

// failedTasks takes as input a map of worker id to worker status. It
// updates the tasks status and returns a list of ids of failed tasks
func (m *tasksManager) failedTasks(wrkrs map[int32]workerStatus) []int32 {
	res := []int32{}
	// TODO: implement me
	return res
}

// task returns a pointer to the task with the specified id. This method will
// panic if no task with the specified id exists
func (m *tasksManager) task(tskID int32) *task {
	if _, ok := m.tsks[tskID]; !ok {
		panic(fmt.Sprintf("task: cannot find task with id: %d", tskID))
	}
	return m.tsks[tskID]
}
