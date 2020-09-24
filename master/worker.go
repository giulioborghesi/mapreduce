package master

// worker represents a MapReduce worker. A MapReduce worker is uniquely
// identified by a worker ID and by a unique address, and its status is
// described by a WorkerStatus object
type worker struct {
	id     int32
	addr   string
	status workerStatus
}
