package service

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/giulioborghesi/mapreduce/roles"
)

func writeIntermediateFile(dict map[string][]string, taskID uint64) error {
	// Create output file
	filePath := "/Users/giulioborghesi/tmp/" + strconv.Itoa(int(taskID))
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Sort keys
	sortedKeys := []string{}
	for key := range dict {
		sortedKeys = append(sortedKeys, key)
	}
	sort.StringSlice(sortedKeys).Sort()

	// Write data to file, sorted by keys
	writer := bufio.NewWriter(f)
	for _, key := range sortedKeys {
		for _, value := range dict[key] {
			line := key + " " + value + "\n"
			_, err := writer.WriteString(line)
			if err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return nil
}

// MapRequestContext groups the parameters that must be supplied to a Map RPC
// call in a single object
type MapRequestContext struct {
	TaskID   uint64
	FilePath string
}

// MapService represents a MapReduce map service
type MapService struct{}

// Map implements a MapReduce map service endpoint. The service takes as input
// a path to a file containing a list of input records and generates an
// intermediate file of sorted key-value pairs.
func (srvc *MapService) Map(ctx *MapRequestContext, _ *Void) error {
	f, err := os.Open(ctx.FilePath)
	if err != nil {
		return err
	}

	defer f.Close()
	reader := bufio.NewReader(f)

	dict := make(map[string][]string)
	mapper := roles.Mapper{}
	for {
		l, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		mapper.Map(l, dict)
	}

	return writeIntermediateFile(dict, ctx.TaskID)
}
