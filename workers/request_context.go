package workers

// RequestContext holds the parameters needed to execute a Mapper / Reducer RPC
// call. Idx is the task number within its group, while Cnt is the number of
// producer / consumer, depending on the context
type RequestContext struct {
	Idx                   int
	MapperCnt, ReducerCnt int
	File                  string
}
