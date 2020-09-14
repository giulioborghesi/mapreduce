package service

import (
	"io"
	"net/http"
	"os"
	"strings"
)

const (
	rootPath = "/Users/giulioborghesi/tmp/"
)

// SendData serves local files download requests. It searches for the requested file
// under rootPath. If the requested file does not exist, it returns an HTTP.StatusNotFound
// error. Since SendData is only accessible by internal services, it is assumed that
// these services are not acting maliciously, and that requests are well formed
func SendData(w http.ResponseWriter, r *http.Request) {
	path := rootPath + strings.TrimPrefix(strings.TrimLeft(r.URL.Path, "/"), "data/")

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
