package master

import (
	"sync"

	"github.com/giulioborghesi/mapreduce/utils"
)

type tasksScheduler struct {
	ws utils.Stack
	tq utils.Queue
	sync.Mutex
}

func makeTasksScheduler(wrkrs []worker, tsks []task) *tasksScheduler {
	ts := new(tasksScheduler)
	for _, wrkr := range wrkrs {
		ts.addWorker(wrkr.id)

	}

	for _, tsk := range tsks {
		ts.addTask(tsk.id, tsk.priority)
	}
	return ts
}

// addTask adds a task to the tasks queue
func (ts *tasksScheduler) addTask(id int32, priority int8) {
	ts.Lock()
	defer ts.Unlock()
	ts.tq.Push(utils.QueueItem{ID: id, Priority: priority})
}

// addWorker adds a worker to the available workers stack
func (ts *tasksScheduler) addWorker(id int32) {
	ts.Lock()
	defer ts.Unlock()
	ts.ws.Push(id)
}

// hasReadyTask checks whether the scheduler has a task ready to be executed.
// It returns true if the answer is positive and false otherwise
func (ts *tasksScheduler) hasReadyTask() bool {
	ts.Lock()
	defer ts.Unlock()
	return ts.tq.Len() > 0 && ts.ws.Len() > 0
}

// nextTask returns the ID of the next task to be executed and the ID of the
// worker where such task should be executed.
func (ts *tasksScheduler) nextTask() (int32, int32) {
	ts.Lock()
	defer ts.Unlock()

	if ts.tq.Len() == 0 || ts.ws.Len() == 0 {
		panic("nexttask: no task ready to be executed")
	}

	tID := ts.tq.Pop().(utils.QueueItem).ID
	wID := ts.ws.Pop().(int32)
	return tID, wID
}
