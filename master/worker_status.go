package master

const (
	// healthy means the worker can be used to execute tasks
	healthy = iota
	// dead means the worker is down and cannot be used anymore
	dead
)

// workerStatus summarizes the status of a MapReduce worker
type workerStatus int8
