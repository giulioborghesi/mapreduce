package master

import (
	"sync"

	"github.com/giulioborghesi/mapreduce/utils"
	"github.com/giulioborghesi/mapreduce/workers"
)

const (
	maxGoRoutines = 20
	sleepFact     = 500
)

// Coordinator manages workers and coordinates tasks execution
type Coordinator struct {
	done bool
	file string
	ts   tasksScheduler
	tm   tasksManager
	wm   workersManager
	cv   *sync.Cond
}

// createMapReduceTasks creates the MapReduce tasks for the MapReduce
// computation
func createMapReduceTasks(mapperCnt, reducerCnt int) []task {
	tsks := make([]task, 0, mapperCnt+reducerCnt)
	for idx := 0; idx < mapperCnt; idx++ {
		id := int32(idx)
		tsks = append(tsks, makeMapperTask(id, idx, mapperCnt, reducerCnt))
	}

	for idx := 0; idx < reducerCnt; idx++ {
		id := int32(idx + mapperCnt)
		tsks = append(tsks, makeReducerTask(id, idx, mapperCnt, reducerCnt))
	}
	return tsks
}

// createMapReduceWorkers creates the MapReduce workers for the MapReduce
// computation
func createMapReduceWorkers(addrs []string) []worker {
	wrkrs := make([]worker, 0, len(addrs))
	for idx, addr := range addrs {
		id := int32(idx)
		wrkrs = append(wrkrs, worker{id: id, addr: addr, status: healthy})
	}
	return wrkrs
}

// MakeCoordinator initializes and returns a task coordinator
func MakeCoordinator(addrs []string, file string,
	mapperCnt, reducerCnt int) *Coordinator {
	tsks := createMapReduceTasks(mapperCnt, reducerCnt)
	wrkrs := createMapReduceWorkers(addrs)

	c := new(Coordinator)
	c.done = false
	c.file = file
	c.tm = *makeTasksManager(tsks)
	c.wm = *makeWorkersManager(wrkrs)
	c.ts = *makeTasksScheduler(wrkrs, tsks)
	c.cv = sync.NewCond(new(sync.Mutex))
	return c
}

// Run starts the MapReduce computation on the Master side
func (c *Coordinator) Run() {
	for i := 0; i < utils.Min(maxGoRoutines, c.wm.activeWorkers()); i++ {
		c.executeTask()
	}

	for {
	}
}

// executeTask pops tasks from the queue and executes them
func (c *Coordinator) executeTask() {
	for {
		// Wait until a task is ready to be scheduled. Return early if
		// MapReduce job has completed
		c.cv.L.Lock()
		for !c.ts.hasReadyTask() {
			c.cv.Wait()
			if c.done {
				return
			}
		}

		// Fetch next task to be executed, release lock and update task status
		tskID, wrkrID := c.ts.nextTask()
		c.cv.L.Unlock()
		c.tm.assignWorkerToTask(wrkrID, tskID)

		// Create client with timeout. On error, mark worker as dead
		addr := c.wm.worker(wrkrID).addr
		client, err := utils.DialHTTP("tcp", addr)
		if err != nil {
			c.wm.reportFailedWorker(wrkrID)
			continue
		}

		// Prepare and submit request
		tsk := c.tm.task(tskID)
		ctx := workers.RequestContext{Idx: tsk.idx, MapperCnt: tsk.mapperCnt,
			ReducerCnt: tsk.reducerCnt, File: c.file}
		reply := new(workers.Status)
		call := client.Go(tsk.method, ctx, reply, nil)

		// Wait for task to complete
		res := <-call.Done
		if res.Error != nil {
			c.wm.reportFailedWorker(wrkrID)
			continue
		}

		// Update task status and insert worker back into task scheduler
		tskStatus := *res.Reply.(*workers.Status)
		c.tm.updateTaskStatus(tskStatus, tskID)
		c.ts.addWorker(wrkrID)
	}
}
