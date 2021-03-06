package workers

import (
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/giulioborghesi/mapreduce/common"
)

const (
	prefixPath = "/Users/giulioborghesi/tmp/mapper/"
)

// SendData serves local files download requests. It searches for the requested
// file under rootPath. If the requested file does not exist, it returns an
// HTTP.StatusNotFound error. Since SendData is only accessible by internal
// services, it is assumed that these services are not acting maliciously, and
// that requests are well formed
func SendData(w http.ResponseWriter, r *http.Request) {
	path := prefixPath + strings.TrimPrefix(strings.TrimLeft(r.URL.Path, "/"),
		"data/")

	f, err := os.Open(path)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Add("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, f)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// UpdateSources updates the mapping from mapper task idx to worker address
// with the latest information received from the master
func (srvc *MapReduceService) UpdateSources(ctx *UpdateRequestContext,
	_ *Void) error {
	srvc.mu.Lock()
	defer srvc.mu.Unlock()

	if _, ok := srvc.tsk2host[ctx.File]; !ok {
		srvc.tsk2host[ctx.File] = make(map[int]common.Host)
	}

	for idx, host := range ctx.Hosts {
		srvc.tsk2host[ctx.File][idx] = host
	}
	return nil
}
