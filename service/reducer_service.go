package service

import (
	"fmt"
	"io"
	"os"

	"github.com/giulioborghesi/mapreduce/roles"
	"github.com/giulioborghesi/mapreduce/utils"
)

// ReduceRequestContext groups the parameters that must be supplied to a Reduce
// RPC call in a single object
type ReduceRequestContext struct {
	FilePaths []string
}

// ReduceService represents a MapReduce reduce service
type ReduceService struct {
	dp *utils.DataProvisioner
}

// Reduce implements a MapReduce reduce service endpoint. The service takes
// as input a path to a file containing a list of sorted key-value pairs and
// generate a file of sorted processed key-value pairs
func (srvc *ReduceService) Reduce(ctx *ReduceRequestContext, s *Status) error {
	its := []io.Reader{}
	for _, filePath := range ctx.FilePaths {
		f, err := os.Open(filePath)
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
		key, it := kvIt.Next()

		// Reduce values
		res, err := reducer.Reduce(key, it)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s\n", key, res)
	}

	(*s) = Status(true)
	return nil
}
