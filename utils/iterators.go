package utils

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// inputIterator represents an iterator over the key-value pairs stored in a
// input stream that satisfies the io.Reader interface
type inputIterator struct {
	reader     *bufio.Reader
	key, value string
	end        bool
}

// makeInputIterator creates and initializes a pointer to a new fileIterator
// object, as well as initializing the first key-value pair
func makeInputIterator(r io.Reader) (*inputIterator, error) {
	it := new(inputIterator)
	it.reader = bufio.NewReader(r)
	it.end = false

	if err := it.next(); err != nil {
		return nil, err
	}
	return it, nil
}

// next fetches the next key-value pair if available, otherwise it sets the end
// flag to true if the EOF condition has been reached
func (it *inputIterator) next() error {
	if it.end {
		return nil
	}

	s, err := it.reader.ReadString('\n')
	if err == io.EOF {
		it.end = true
		return nil
	}

	if err != nil {
		return err
	}

	vals := strings.Split(s, " ")
	it.key = vals[0]
	it.value = strings.TrimRight(vals[1], "\n")
	return nil
}

// ValueIterator implements an iterator over the values associated with a
// single key
type ValueIterator struct {
	Key string
	its []*inputIterator
}

// HasNext returns true if there exists a non-processed value for the
// current key, and false otherwise
func (it *ValueIterator) HasNext() bool {
	return len(it.its) > 0
}

// Next returns the next unprocessed value for the current key. The method
// assumes that such value exists, and will panic otherwise
func (it *ValueIterator) Next() (string, error) {
	if it.HasNext() == false {
		panic(fmt.Sprintf("ValueIterator: no additional value exists for "+
			"key %s", it.Key))
	}

	// Fetch next value
	value := it.its[0].value
	if err := it.its[0].next(); err != nil {
		return "", err
	}

	// Remove iterator if it has exhausted the values for the current key
	if it.its[0].end || it.its[0].key != it.Key {
		len := len(it.its)
		it.its[0], it.its[len-1] = it.its[len-1], it.its[0]
		it.its = it.its[:len-1]
	}
	return value, nil
}

// KeyValueIterator implements an iterator over the key-values pairs extracted
// from several input sources that satisfies the io.Reader interface
type KeyValueIterator struct {
	its []inputIterator
}

// MakeKeyValueIterator creates and initializes a pointer to a new
// KeyValueIterator object
func MakeKeyValueIterator(rs ...io.Reader) (*KeyValueIterator, error) {
	its := []inputIterator{}
	for _, r := range rs {
		it, err := makeInputIterator(r)
		if err != nil {
			return nil, err
		}
		its = append(its, *it)
	}

	return &KeyValueIterator{its: its}, nil
}

// Next returns the key and corresponding values iterator for the next
// unprocessed key-values pair. The method assumes that data has not been fully
// consumed yet, and will panic should this condition not be satisfied
func (kvIt *KeyValueIterator) Next() (string, *ValueIterator) {
	if kvIt.HasNext() == false {
		panic(fmt.Sprintf("KeyValueIterator: no additional key-values exists"))
	}

	key := kvIt.its[0].key
	its := []*inputIterator{}
	for i := range kvIt.its {
		ptrIt := &kvIt.its[i]
		if key == ptrIt.key {
			its = append(its, ptrIt)
		} else if key < ptrIt.key {
			key = ptrIt.key
			its = []*inputIterator{ptrIt}
		}
	}

	return key, &ValueIterator{Key: key, its: its}
}

// HasNext returns true if the iterator still has unprocessed key-values pairs,
// and false otherwise. It also removes the fully processed iterators
func (kvIt *KeyValueIterator) HasNext() bool {
	n := len(kvIt.its)
	for i := len(kvIt.its) - 1; i >= 0; i-- {
		if kvIt.its[i].end == true {
			kvIt.its[i], kvIt.its[n-1] = kvIt.its[n-1], kvIt.its[i]
			n--
		}
	}
	kvIt.its = kvIt.its[:n]
	return len(kvIt.its) > 0
}
