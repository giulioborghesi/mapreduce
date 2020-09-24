package master

const (
	// mapTask is the service method to be used for Map tasks
	mapTask = "MapReduceService.Map"
	// reduceTask is the service method to be used for Reduce tasks
	reduceTask = "MapReduceService.Reduce"
	// statusTask is the service method to be used for Status tasks
	statusTask = "MapReduceService.Status"
	// invalidWorkerID is the ID used for tasks not assigned to a worker yet
	invalidWorkerID = -1
)

// task represents a generic task in a MapReduce computation. Aside
// from storing basic information such as task id, status, priority
// and task type, a task object also stores its position (idx) within
// tasks of the same type and the number of its consumers / producers
type task struct {
	id       int32
	tskID    int32
	idx      int
	cnt      int
	priority int8
	method   string
	status   taskStatus
}

// makeMapperTask creates a new Mapper task
func makeMapperTask(id int32, idx, cnt int) task {
	return task{id: id, tskID: invalidWorkerID, idx: idx, cnt: cnt,
		priority: 0, method: mapTask, status: idle}
}

// makeMapperTask creates a new Reducer task
func makeReducerTask(id int32, idx, cnt int) task {
	return task{id: id, tskID: invalidWorkerID, idx: idx, cnt: cnt,
		priority: 1, method: reduceTask, status: idle}
}
