package utils

import "testing"

func TestStack(t *testing.T) {
	// Create stack and add element to stack
	s := Stack{}
	s.Push(15)
	s.Push(20)

	// Stack size should be 2
	if s.Len() != 2 {
		t.Errorf("Stack size incorrect, got: %d, want: %d.", s.Len(), 2)
	}

	// Pop topmost element
	x := s.Pop().(int)
	if x != 20 {
		t.Errorf("Topmost stack element incorrect, got: %d, wanted: %d", x, 20)
	}

	// Stack size should be 1 now
	if s.Len() != 1 {
		t.Errorf("Stack size incorrect, got: %d, want: %d.", s.Len(), 1)
	}
}
