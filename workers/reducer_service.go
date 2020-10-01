package workers

import (
	"fmt"
	"io"
	"os"

	"github.com/giulioborghesi/mapreduce/roles"
	"github.com/giulioborghesi/mapreduce/utils"
)

// Reduce implements a MapReduce reduce service endpoint. The service processes
// a set of data sources and generates a file of sorted key-value pairs. A
// Reduce task can fail when the intermediate files are not available for too
// many times in a row
func (srvc *MapReduceService) Reduce(ctx *RequestContext, s *Status) error {
	// Initialize return status
	*s = FAILED

	// Provision data
	p := makeDataProvisioner(ctx, srvc)
	paths, err := p.provisionData()
	if err != nil {
		return nil
	}

	// Open files
	its := []io.Reader{}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		its = append(its, f)
	}

	// Create key-values iterator
	kvIt, err := utils.MakeKeyValueIterator(its...)
	if err != nil {
		return err
	}

	reducer := roles.Reducer{}
	for {
		// Check if all data has been processed
		if kvIt.HasNext() == false {
			break
		}
		key, vIt := kvIt.Next()

		// Reduce values
		res, err := reducer.Reduce(key, vIt)
		if err != nil {
			return err
		}

		// Print values. TODO: remove and store to file
		fmt.Printf("%s: %s\n", key, res)
	}
	*s = SUCCESS
	return nil
}
