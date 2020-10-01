package master

import (
	"log"
	"time"

	"github.com/giulioborghesi/mapreduce/common"
	"github.com/giulioborghesi/mapreduce/utils"
	"github.com/giulioborghesi/mapreduce/workers"
)

const (
	maxGoRoutines     = 20
	sleepTimeInMs     = 500
	taskDeadlineInMin = 10
)

// Coordinator manages workers and coordinates tasks execution
type Coordinator struct {
	done bool
	file string
	ts   tasksScheduler
	tm   tasksManager
	wm   workersManager
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
	return c
}

// Run starts the MapReduce computation on the Master side
func (c *Coordinator) Run() {
	for i := 0; i < utils.Min(maxGoRoutines, c.wm.activeWorkers()); i++ {
		go c.executeTask()
	}

	for {
		// Nothing to do if no worker is available
		if c.wm.activeWorkers() == 0 {
			log.Fatalln("run: no worker left: aborting mapreduce computation")
		}

		// Update workers and tasks status
		wrkrsStatus := c.wm.updatedWorkersStatus()
		tsksStatus := c.tm.updatedTasksStatus(wrkrsStatus)

		// Reschedule tasks if needed
		for tskID, tskStatus := range tsksStatus {
			if tskStatus == failed {
				c.ts.addTask(tskID, c.tm.task(tskID).priority)
			}
		}

		// Interrupt the computation if all reduce tasks have completed
		if c.tm.reduceTasksLeft() == 0 {
			break
		}

		// Update the data sources
		c.updateDataSources(tsksStatus, wrkrsStatus)

		// Some tasks have not completed yet. Wait and then repeat
		log.Printf("Reduce tasks not completed yet: %d",
			c.tm.reduceTasksLeft())
		time.Sleep(sleepTimeInMs * time.Millisecond)
	}
	log.Println("MapReduce computation completed!")
}

// executeTask pops tasks from the queue and executes them
func (c *Coordinator) executeTask() {
	for {
		// Wait until a task is ready to be scheduled. Return early if
		// MapReduce job has completed
		c.ts.cv.L.Lock()
		for !c.ts.hasReadyTask() {
			c.ts.cv.Wait()
			if c.done {
				return
			}
		}

		// Fetch next task to be executed, release lock and update task status
		tskID, wrkrID := c.ts.nextTask()
		c.ts.cv.L.Unlock()
		c.tm.assignWorkerToTask(wrkrID, tskID)

		// Create client with timeout. On error, mark worker as dead
		addr := c.wm.worker(wrkrID).addr
		client, err := utils.DialHTTP("tcp", addr, taskDeadlineInMin*
			time.Minute)
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
		client.Close()
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

// updateDataSources updates the data sources in all workers
func (c *Coordinator) updateDataSources(tsksStatus map[int32]taskStatus,
	wrkrsStatus map[int32]workerStatus) {
	// Prepare message
	hosts := make(map[int]common.Host)
	for tskID, tskStatus := range tsksStatus {
		tsk := c.tm.task(tskID)
		if tsk.method == reduceTask {
			continue
		}
		if tskStatus == done {
			wrkr := c.wm.worker(tsk.wrkrID)
			hosts[tsk.idx] = common.Host(wrkr.addr)
		} else {
			hosts[tsk.idx] = ""
		}
	}

	// Send the message to all workers asynchronously
	chans := make(map[int32]chan workers.Void)
	ctx := workers.UpdateRequestContext{File: c.file, Hosts: hosts}
	for wrkrID := range wrkrsStatus {
		wrkr := c.wm.worker(wrkrID)
		if wrkr.status == dead {
			continue
		}

		rchn := make(chan workers.Void)
		chans[wrkrID] = rchn
		go func() {
			client, err := utils.DialHTTP("tcp", wrkr.addr,
				statusDeadlineInMs*time.Millisecond)
			if err != nil {
				rchn <- workers.Void{}
				return
			}

			client.Call(dataSourcesUpdateTask, &ctx, new(workers.Void))
			client.Close()
			rchn <- workers.Void{}
		}()
	}

	// Force synchronization
	for _, rchn := range chans {
		<-rchn
	}
}
