package workers

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/giulioborghesi/mapreduce/roles"
	"github.com/giulioborghesi/mapreduce/utils"
)

const (
	mapperPath = "/Users/giulioborghesi/tmp/mapper/"
)

// writeFile writes the intermediate key / value pairs to file. The filename
// has the following format: {task index}.{producer index}.
func writeFile(kvPairs map[string][]string, nameBase string,
	fileIdx int) error {
	path := mapperPath + nameBase + "." + strconv.Itoa(fileIdx)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Sort keys
	sortedKeys := make([]string, 0, len(kvPairs))
	for key := range kvPairs {
		sortedKeys = append(sortedKeys, key)
	}
	sort.StringSlice(sortedKeys).Sort()

	// Write data to file, sorted by keys
	writer := bufio.NewWriter(f)
	for _, key := range sortedKeys {
		for _, value := range kvPairs[key] {
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

// writeIntermediateFiles partitions the key / value pairs into partitions
// based on a user-supplied partition functions. Individual partitions are
// then written to file by calling writeFile
func writeIntermediateFiles(kvPairs map[string][]string, nameBase string,
	parts int) error {
	// Partition key / value pairs
	splitKvPairs := make(map[int]map[string][]string)
	for k, v := range kvPairs {
		i := roles.Partition(k, parts)
		if _, ok := splitKvPairs[i]; !ok {
			splitKvPairs[i] = make(map[string][]string)
		}
		splitKvPairs[i][k] = v
	}

	// Write one file for each partition of the key / value pairs
	for i := 0; i < parts; i++ {
		if err := writeFile(splitKvPairs[i], nameBase, i); err != nil {
			return err
		}
	}
	return nil
}

// Map implements a MapReduce map service endpoint. The service takes as input
// a path to a file containing a list of input records and generates an
// intermediate file of sorted key-value pairs. A Map task cannot be preempted
// and thus is always successfull, unless an irreversible error occur; in that
// case, however, the return status is ignored and thus its value is irrelevant
func (srvc *MapReduceService) Map(ctx *RequestContext, s *Status) error {
	*s = SUCCESS
	f, err := os.Open(ctx.File)
	if err != nil {
		return err
	}

	defer f.Close()
	reader := bufio.NewReader(f)

	kvPairs := make(map[string][]string)
	mapper := roles.Mapper{}
	for {
		l, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		mapper.Map(l, kvPairs)
	}

	nameBase := utils.GetIntermediateFilePrefix(ctx.File, ctx.Idx)
	return writeIntermediateFiles(kvPairs, nameBase, ctx.ReducerCnt)
}
