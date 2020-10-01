package workers

import "github.com/giulioborghesi/mapreduce/common"

// RequestContext holds the parameters needed to execute a Mapper / Reducer RPC
// call. Idx is the task number within its group, while Cnt is the number of
// producer / consumer, depending on the context
type RequestContext struct {
	Idx                   int
	MapperCnt, ReducerCnt int
	File                  string
}

// UpdateRequestContext holds the parameters needed to update the mapper task /
// worker address with latest information received from the master
type UpdateRequestContext struct {
	File  string
	Hosts map[int]common.Host
}
