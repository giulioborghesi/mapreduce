package utils

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const (
	localPathPrefix = "/Users/giulioborghesi/tmp/"
	idle            = iota
	inprogress
	done
	failed
)

type status int32

// DataSource groups the information needed to provision data from a remote host
type DataSource struct {
	file string
	host string
	port string
}

// DataProvisioner allows a service to provision data stored in remote hosts. The
// provisioner will contact the host storing the data and download it locally
type DataProvisioner struct {
	status map[string]status
	queue  list.List
	sync.Mutex
}

// MakeDataProvisioner initializes the data provisioner from a list of data sources
func MakeDataProvisioner(dss []DataSource) *DataProvisioner {
	dp := &DataProvisioner{}
	for _, ds := range dss {
		localPath := localPathPrefix + ds.file
		if _, err := os.Stat(localPath); os.IsNotExist(err) {
			dp.status[ds.file] = idle
			if ds.host != "" && ds.port != "" {
				dp.queue.PushBack(ds)
			}
		} else {
			dp.status[ds.file] = done
		}
	}

	return dp
}

// AddSource adds a data source to the DataProvisioner object
func (dp *DataProvisioner) AddSource(ds DataSource) {
	dp.Lock()
	defer dp.Unlock()

	s, ok := dp.status[ds.file]
	if !ok || s != idle {
		return
	}
	dp.queue.PushFront(ds)
}

// checkAndSetStatus sets the status of a data source to the client-specified one. The operation
// is successfull only if the current status of the data source is either idle or failed
func (dp *DataProvisioner) checkAndSetStatus(s status, ds DataSource) error {
	dp.Lock()
	defer dp.Unlock()

	if s := dp.status[ds.file]; s == inprogress || s == done {
		return errors.New("DataProvisioner: data provisioning already initiated / completed")
	}
	dp.status[ds.file] = s
	return nil
}

func (dp *DataProvisioner) fetchData(ds DataSource) (string, error) {
	// Before proceeding, ensure data provisioning has not been initiated / completed already
	if err := dp.checkAndSetStatus(inprogress, ds); err != nil {
		return "", err
	}

	// Should an error occur, set status to failed
	var s status = failed
	defer func() {
		dp.setStatus(ds, s)
	}()

	// Fetch data from remote server
	u := url.URL{Host: ds.host + ":" + ds.port, Scheme: "https", Path: "data/" + ds.file}
	resp, err := http.Get(u.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Create output file. File closure not deferred intentionally
	localPath := localPathPrefix + ds.file
	f, err := os.Create(localPath)
	if err != nil {
		return "", err
	}

	// Copy data to local file
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		f.Close()
		os.Remove(localPath)
		return "", err
	}
	f.Close()

	// Data copied successfully to local file, set status to done
	s = done
	return localPath, nil
}

// nextSource fetches the next data source to be provisioned. The method will panic if
// no data source is currently available for processing
func (dp *DataProvisioner) nextSource() DataSource {
	dp.Lock()
	defer dp.Unlock()

	if dp.queue.Len() == 0 {
		panic(fmt.Sprintf("DataProvisioner: cannot extract data source from empty queue\n"))
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

		fs := dp.nextSource()
		localPath, err := dp.fetchData(fs)
		if err != nil {
			continue
		}

		localPaths = append(localPaths, localPath)
	}

	return localPaths
}

// setStatus sets the data source status to a client-specified value
func (dp *DataProvisioner) setStatus(ds DataSource, s status) {
	dp.Lock()
	defer dp.Unlock()

	dp.status[ds.file] = s
}
