package master

const (
	// idle means that the task has not been launched yet
	idle = iota
	// inProgress means that the task is in progress
	inProgress
	// done means that the task has completed successfully
	done
	// failed means that the task failed during execution
	failed
)

// taskStatus summarizes the status of a task
type taskStatus int8
