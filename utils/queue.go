package utils

// QueueItem represents an item that can be stored in a Queue queue
type QueueItem struct {
	ID       int32
	Priority int8
}

// Queue implements a queue whose elements are sorted by priority
type Queue []QueueItem

// Len returns the size of the queue
func (t Queue) Len() int {
	return len(t)
}

// Less compares two queue elements and returns true if the first
// element is smaller than the second element
func (t Queue) Less(i, j int) bool {
	return t[i].Priority < t[j].Priority
}

// Swap swaps the position of two elmements in the queue
func (t Queue) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Push adds an element to the queue
func (t *Queue) Push(x interface{}) {
	*t = append(*t, x.(QueueItem))
}

// Pop removes an element from the queue
func (t *Queue) Pop() interface{} {
	n := len(*t)
	res := (*t)[n-1]
	*t = (*t)[:n-1]
	return res
}
