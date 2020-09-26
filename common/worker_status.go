package common

const (
	// IDLE status code, no explanation needed
	IDLE = iota
	// INPROGRESS status code, no explanation needed
	INPROGRESS
	// DONE status code, no explanation needed
	DONE
	// UNAVAILABLE status code, worker no longer available
	UNAVAILABLE
)

var (
	// IdleStatus is used to specify the status of an idle worker
	IdleStatus = WorkerStatus{status: IDLE}
	// UnavailableStatus is used to specify the status of a worker
	// that is no longer available
	UnavailableStatus = WorkerStatus{status: UNAVAILABLE}
)

// StatusCode is a type alias for a WorkerStatus status code
type StatusCode int

// WorkerStatus summarizes the status of a MapReduce worker
type WorkerStatus struct {
	status       StatusCode
	dependencies []int32
}

// Reset resets a worker status to its default value
func (s *WorkerStatus) Reset() {
	s.status = IDLE
	s.dependencies = []int32{}
}

// IsUnavailable returns true if the worker's status is UNAVAILABLE
// and false otherwise
func (s *WorkerStatus) IsUnavailable() bool {
	return s.status == UNAVAILABLE
}

// IsDone returns true if the worker's status is DONE and false
// otherwise
func (s *WorkerStatus) IsDone() bool {
	return s.status == DONE
}
