package roles

import (
	"strings"

	"github.com/giulioborghesi/mapreduce/utils"
)

// Mapper is a struct that implements the MapReduce map function
type Mapper struct{}

// Map implements the Map function used by MapReduce to map values to
// a dictionary of key-values pairs
func (m *Mapper) Map(val string, dict map[string][]string) {
	vals := strings.Split(val, " ")
	for _, s := range vals {
		if len(s) > 0 {
			ns := utils.NormalizeString(s)
			dict[ns] = append(dict[ns], "1")
		}
	}
}
