package roles

import (
	"strconv"

	"github.com/giulioborghesi/mapreduce/utils"
)

// Reducer is a struct that implements the MapReduce reduce function
type Reducer struct{}

// Reduce implements the reduce function used by MapReduce to reduce the
// values that maps to the same key. The Reduce function implemented here
// is used alongside the Map function to count the occurrence of words in
// a text file
func (m *Reducer) Reduce(key string, it *utils.ValueIterator) (string, error) {
	res := 0
	for {
		if !it.HasNext() {
			break
		}

		val, err := it.Next()
		if err != nil {
			return "", err
		}

		ival, err := strconv.Atoi(val)
		if err != nil {
			return "", err
		}
		res += ival
	}

	return strconv.Itoa(res), nil

}
