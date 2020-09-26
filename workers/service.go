package workers

// Void is a dummy type used for empty RPC arguments
type Void struct{}

// MapReduceService implements a MapReduce RPC service
type MapReduceService struct {
	tsk2addr map[string]map[int]string
}
