package workers

const (
	// SUCCESS is used when the RPC executed successfully
	SUCCESS = iota
	// FAILED is used when the RPC was preempted
	FAILED
)

// Status indicates whether an RPC call to one of the endpoints of the
// MapReduce service was successful or not
type Status int8
