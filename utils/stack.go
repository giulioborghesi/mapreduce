package utils

// Stack implements a stack for storing generic objects
type Stack []interface{}

// Push adds an object to the top of the stack
func (s *Stack) Push(x interface{}) {
	*s = Stack(append([]interface{}(*s), x))
}

// Pop removes the topmost object from the stack and returns it
func (s *Stack) Pop() interface{} {
	y := []interface{}(*s)
	n := len(y)
	x := y[n-1]
	y = y[:n-1]
	*s = Stack(y)
	return x
}

// Len returns the number of elements in the stack
func (s *Stack) Len() int {
	y := []interface{}(*s)
	return len(y)
}
