package utils

import "container/heap"

// QueueItem represents an item that can be stored in a Queue queue
type QueueItem struct {
	ID       int32
	Priority int8
}

// queueStorage represents the backing storage for a priority queue
type queueStorage []QueueItem

// Len returns the size of the queue
func (t queueStorage) Len() int {
	return len(t)
}

// Less compares two queue elements and returns true if the first
// element is smaller than the second element
func (t queueStorage) Less(i, j int) bool {
	return t[i].Priority < t[j].Priority
}

// Swap swaps the position of two elmements in the queue
func (t queueStorage) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Push adds an element to the backing storage for the priority queue
func (t *queueStorage) Push(x interface{}) {
	*t = append(*t, x.(QueueItem))
}

// Pop removes an element from the backing storage for the priority queue
func (t *queueStorage) Pop() interface{} {
	n := len(*t)
	res := (*t)[n-1]
	*t = (*t)[:n-1]
	return res
}

// Queue implements a queue whose elements are sorted by increasing priority
type Queue struct {
	s queueStorage
}

// Push adds an element to the priority queue
func (q *Queue) Push(x interface{}) {
	heap.Push(&q.s, x)
}

// Pop removes the element with the highest priority from the priority queue
func (q *Queue) Pop() interface{} {
	return heap.Pop(&q.s)
}

// Len returns the number of elements in the priority queue
func (q *Queue) Len() int {
	return q.s.Len()
}
