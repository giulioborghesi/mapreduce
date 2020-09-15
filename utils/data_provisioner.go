package utils

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	pathPrefix = "/Users/giulioborghesi/tmp/"
	idle       = iota
	ready
	inprogress
	done
	failed
)

type status int32

// DataSource groups the information needed to provision data from a remote host
type DataSource struct {
	File string
	Host string
}

// DataProvisioner allows a service to provision data stored in remote hosts. The
// provisioner will contact the host storing the data through HTTP and download it
type DataProvisioner struct {
	host   string
	status map[string]status
	queue  list.List
	sync.Mutex
}

// MakeDataProvisioner initializes the data provisioner from a list of data sources
func MakeDataProvisioner(host string, sources []DataSource) *DataProvisioner {
	dp := &DataProvisioner{host: host, status: make(map[string]status)}
	for _, source := range sources {
		dp.status[source.File] = idle
		if source.Host != "" {
			dp.queue.PushBack(source)
			dp.status[source.File] = ready
		}
	}
	return dp
}

// sameHost checks whether two hosts are the same
func sameHost(hostA string, hostB string) bool {
	hostA = strings.Split(hostA, ":")[0]
	hostB = strings.Split(hostB, ":")[0]
	fmt.Println(hostA, hostB)
	return hostA == hostB
}

// AddSource adds a data source to the DataProvisioner object
func (dp *DataProvisioner) AddSource(ds DataSource) {
	dp.Lock()
	defer dp.Unlock()

	s, ok := dp.status[ds.File]
	if !ok || s == inprogress || s == done {
		return
	}
	dp.queue.PushFront(ds)
	dp.status[ds.File] = ready
}

// initStatus sets the status of a data source to in progress. The operation is
// successfull only if the current status of the data source is either idle or failed
func (dp *DataProvisioner) initStatus(source DataSource) error {
	dp.Lock()
	defer dp.Unlock()

	if s := dp.status[source.File]; s == inprogress || s == done {
		return errors.New("DataProvisioner: data provisioning already initiated / completed")
	}
	dp.status[source.File] = inprogress
	return nil
}

func (dp *DataProvisioner) fetchData(source DataSource) (string, error) {
	// Before proceeding, ensure data provisioning has not been initiated / completed already
	if err := dp.initStatus(source); err != nil {
		return "", err
	}

	// Should an error occur, set status to failed
	var s status = failed
	defer func() {
		dp.setStatus(source, s)
	}()

	// If data is stored locally, only check whether file exists and return
	if sameHost(dp.host, source.Host) == true {
		path := pathPrefix + source.File
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return "", err
		}
		s = done
		return path, nil
	}

	// Data not stored locally, fetch it from remote server
	u := url.URL{Host: source.Host, Scheme: "http", Path: "data/" + source.File}
	resp, err := http.Get(u.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Create output file. File closure not deferred intentionally
	path := pathPrefix + source.File
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}

	// Copy data to local file
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	f.Close()

	// Data copied successfully to local file, set status to done
	s = done
	return path, nil
}

// MissingFiles returns a list of files for which either download failed or no information
// is available. Files being downloaded or for which location information is available are
// not returned
func (dp *DataProvisioner) MissingFiles() []string {
	dp.Lock()
	defer dp.Unlock()

	res := []string{}
	for k, v := range dp.status {
		if v == idle || v == failed {
			res = append(res, k)
		}
	}
	return res
}

// nextSource fetches the next data source to be provisioned. The method will panic if
// no data source is currently available for processing
func (dp *DataProvisioner) nextSource() DataSource {
	dp.Lock()
	defer dp.Unlock()

	if dp.queue.Len() == 0 {
		panic(fmt.Sprintf("DataProvisioner: nextSource: queue is empty\n"))
	}

	ds := (dp.queue.Back().Value).(DataSource)
	dp.queue.Remove(dp.queue.Back())
	return ds
}

// ProvisionData provisions the data stored in the remote hosts and returns a list
// of paths to the locally stored data files
func (dp *DataProvisioner) ProvisionData() []string {
	localPaths := []string{}
	for {
		if len(localPaths) == len(dp.status) {
			break
		}

		for dp.queue.Len() == 0 {
			time.Sleep(250 * time.Millisecond)
		}

		source := dp.nextSource()
		localPath, err := dp.fetchData(source)
		if err != nil {
			continue
		}

		localPaths = append(localPaths, localPath)
	}

	return localPaths
}

// setStatus sets the data source status to a client-specified value
func (dp *DataProvisioner) setStatus(source DataSource, s status) {
	dp.Lock()
	defer dp.Unlock()

	dp.status[source.File] = s
}
