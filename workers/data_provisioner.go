package workers

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/giulioborghesi/mapreduce/utils"

	"github.com/giulioborghesi/mapreduce/common"
)

const (
	reducerPath = "/Users/giulioborghesi/tmp/reducer/"
	maxAttempts = 16
	idle        = iota
	done
	failed
)

type sourceStatus int

// DataSource groups the information needed to provision data from a remote
// host
type dataSource struct {
	idx    int
	host   common.Host
	status sourceStatus
}

// DataProvisioner allows a service to provision data stored in remote hosts.
// The provisioner will contact the hosts storing the data through HTTP
// requests and download it
type dataProvisioner struct {
	file    string
	idx     int
	sources map[int]*dataSource
	queue   list.List
	srvc    *MapReduceService
}

// makeDataProvisioner initializes the data provisioner from a request context
// object and a MapReduce service instance
func makeDataProvisioner(ctx *RequestContext,
	srvc *MapReduceService) *dataProvisioner {
	p := &dataProvisioner{file: ctx.File, sources: make(map[int]*dataSource),
		idx: ctx.Idx, srvc: srvc}
	for i := 0; i < ctx.MapperCnt; i++ {
		p.sources[i] = &dataSource{idx: i, status: idle}
	}
	return p
}

// addTask adds a task to the download queue
func (p *dataProvisioner) addTask(tsk int) {
	p.queue.PushBack(tsk)
}

// dataSource returns the data source for task with index idx
func (p *dataProvisioner) dataSource(idx int) dataSource {
	if _, ok := p.sources[idx]; !ok {
		panic(fmt.Sprintf("datasource: task index invalid: %d", idx))
	}
	return *p.sources[idx]
}

// fetchData downloads the requested file from a data source
func (p *dataProvisioner) fetchData(src dataSource) string {
	// Set source status on return
	var s sourceStatus = failed
	defer func() {
		p.sources[src.idx].status = s
	}()

	// Construct URL
	host := string(src.host)
	dataPath := "data/" + utils.GetIntermediateFilePrefix(p.file, src.idx) +
		"." + strconv.Itoa(p.idx)
	u := url.URL{Host: host, Scheme: "http", Path: dataPath}

	// Fetch file through HTTP
	resp, err := http.Get(u.String())
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	// Create output file. File closure not deferred intentionally
	filePath := reducerPath + utils.GetIntermediateFilePrefix(p.file, p.idx) +
		"." + strconv.Itoa(src.idx)

	f, err := os.Create(filePath)
	if err != nil {
		return ""
	}

	// Copy data to local file and return
	_, err = io.Copy(f, resp.Body)
	f.Close()
	if err != nil {
		os.Remove(filePath)
		return ""
	}
	s = done
	return filePath
}

// fetchDataFromSources downloads all the files for which a data source is
// available
func (p *dataProvisioner) fetchDataFromSources() []string {
	res := make([]string, 0)
	for {
		if p.queue.Len() == 0 {
			break
		}

		src := p.dataSource(p.nextTask())
		if path := p.fetchData(src); path != "" {
			res = append(res, path)
		}
	}
	return res
}

// nexttask fetches the next task source to be provisioned. The method will
// panic if the task queue is empty
func (p *dataProvisioner) nextTask() int {
	if p.queue.Len() == 0 {
		panic(fmt.Sprintln("nexttask: empty queue"))
	}

	src := (p.queue.Back().Value).(int)
	p.queue.Remove(p.queue.Back())
	return src
}

// provisionData provisions the data stored in the remote hosts and returns a
// list of paths to the locally stored data files on success.
func (p *dataProvisioner) provisionData() ([]string, error) {
	paths := []string{}

	for {
		// Download available files
		paths = append(paths, p.fetchDataFromSources()...)
		if len(paths) == len(p.sources) {
			break
		}

		// Attempt to update data sources and fail if unsuccessful
		for i, fact := 0, 1; i <= maxAttempts; i++ {
			for idx, source := range p.sources {
				if source.status == done {
					continue
				}

				host := p.srvc.host(p.file, idx)
				if source.host == host || host == "" {
					continue
				}

				source.host = host
				source.status = idle
				p.addTask(idx)
			}

			if p.queue.Len() > 0 {
				break
			}

			if i == maxAttempts {
				return nil, errors.New("provisiondata: data unavailable")
			}
			time.Sleep(time.Duration(fact) * time.Millisecond)
			fact *= 2
		}
	}
	return paths, nil
}
