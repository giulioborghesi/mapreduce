package master

import (
	"net/rpc"
	"sync"

	"github.com/giulioborghesi/mapreduce/service"
)

const (
	sleepFact = 500
)

// Coordinator manages workers and coordinates tasks execution
type Coordinator struct {
	done bool
	ts   tasksScheduler
	tm   tasksManager
	wm   workersManager
	cv   sync.Cond
}

// MakeCoordinator initializes and returns a task coordinator
func MakeCoordinator(addrs []string, file string,
	mapCnt, redCnt int) *Coordinator {
	tsks := []task{}
	for idx := 0; idx < mapCnt; idx++ {
		id := int32(idx)
		tsks = append(tsks, makeMapperTask(id, idx, redCnt))
	}

	for idx := 0; idx < redCnt; idx++ {
		id := int32(idx + mapCnt)
		tsks = append(tsks, makeReducerTask(id, idx, mapCnt))
	}

	c := new(Coordinator)
	c.tm = *makeTasksManager(tsks)
	c.wm = *makeWorkersManager(addrs)
	c.done = false
	return c
}

// Run starts the MapReduce computation on the Master side
func (c *Coordinator) Run() {
	go c.executeTask()
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
		args := service.ArgsRPC{Idx: tsk.idx, Cnt: tsk.cnt}
		reply := new(service.Status)
		call := client.Go(tsk.method, args, reply, nil)

		// Wait for task to complete
		res := <-call.Done
		if res.Error != nil {
			c.wm.reportFailedWorker(wrkrID)
			continue
		}

		// Task may be preempted and fail. Mark task as failed if needed
		if *res.Reply.(*service.Status) != service.SUCCESS {
			//			c.tm.reportFailure(tskID, wrkrID)
		} else {
			//			c.tm.reportSuccess(tskID)
		}

		// Insert worker back into scheduler
		c.ts.addWorker(wrkrID)
	}
}
