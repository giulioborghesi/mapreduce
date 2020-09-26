package master

import (
	"fmt"
	"net/rpc"
	"sync"

	"github.com/giulioborghesi/mapreduce/workers"
)

const (
	sleepFact = 500
)

// Coordinator manages workers and coordinates tasks execution
type Coordinator struct {
	done bool
	file string
	ts   tasksScheduler
	tm   tasksManager
	wm   workersManager
	cv   sync.Cond
}

// createMapReduceTasks creates the MapReduce tasks for the MapReduce
// computation
func createMapReduceTasks(mapperCnt, reducerCnt int) []task {
	tsks := make([]task, 0, mapperCnt+reducerCnt)
	for idx := 0; idx < mapperCnt; idx++ {
		id := int32(idx)
		tsks = append(tsks, makeMapperTask(id, idx, reducerCnt))
	}

	for idx := 0; idx < reducerCnt; idx++ {
		id := int32(idx + mapperCnt)
		tsks = append(tsks, makeReducerTask(id, idx, mapperCnt))
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
	return c
}

// Run starts the MapReduce computation on the Master side
func (c *Coordinator) Run() {
	fmt.Println(c.ts.hasReadyTask())
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

		// Create client. On error, mark worker as dead
		addr := c.wm.worker(wrkrID).addr
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			c.wm.reportFailedWorker(wrkrID)
			continue
		}

		// Prepare and submit request
		tsk := c.tm.task(tskID)
		ctx := workers.RequestContext{Idx: tsk.idx, Cnt: tsk.cnt, File: c.file}
		reply := new(workers.Status)
		call := client.Go(tsk.method, &ctx, reply, nil)

		// Wait for task to complete
		res := <-call.Done
		if res.Error != nil {
			c.wm.reportFailedWorker(wrkrID)
			continue
		}

		// Task may be preempted and fail. Mark task as failed if needed
		if *res.Reply.(*workers.Status) != workers.SUCCESS {
			//			c.tm.reportFailure(tskID, wrkrID)
		} else {
			//			c.tm.reportSuccess(tskID)
		}

		// Insert worker back into scheduler
		c.ts.addWorker(wrkrID)
	}
}