package service

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/giulioborghesi/mapreduce/roles"
	"github.com/giulioborghesi/mapreduce/utils"
)

const (
	// IDLE indicates that the Reduce service is ready to accept a Reduce task
	IDLE = iota

	// PROVISIONING indicates that the Reduce service is provisioning data
	PROVISIONING

	// REDUCING indicates that the Reduce service is performing the reduce phase
	REDUCING

	// DONE indicates that the Reduce service has completed data processing
	DONE

	// ERROR indicates that the Reduce task failed due to an irreversible error
	ERROR
)

// ReduceRequestContext groups the parameters that must be supplied to a Reduce
// service RPC call
type ReduceRequestContext struct {
	ID      int64
	Sources []utils.DataSource
}

// status represents the current status of the Reduce service
type status struct {
	id   int64
	code int8
	mu   sync.Mutex
}

// initStatus initializes the Reduce service status to PROVISIONING. The
// initialization is successfull only if the service is in a IDLE state
func (s *status) initStatus(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.code != IDLE {
		return errors.New("ReduceService: cannot call Reduce on non-idle service")
	}
	s.id = id
	s.setStatus(PROVISIONING)
	return nil
}

// setStatus sets the Reduce service status to the specified value. Although
// not enforced here, code should be one of the exported constant values.
func (s *status) setStatus(code int8) {
	s.code = code
}

// ReduceService represents a MapReduce reduce service
type ReduceService struct {
	Host string
	dp   *utils.DataProvisioner
	status
}

// ReduceServiceStatus summarizes the status of the Reduce service that is returned
// to the master when the Status RPC endpoint is called
type ReduceServiceStatus struct {
	Code         int8
	MissingFiles []string
}

// Reduce implements a MapReduce reduce service endpoint. The service takes
// as input a list of data sources and generate a file of processed key-value
// pairs in lexicographically sorted order. Calls to Reduce cannot be concurrent:
// to avoid this situation, a status object is used that is updated as the computation
// proceeds. The master must read the state at the end of the computation (which
// will be either DONE or ERROR) to reset it and allow another Reduce call
func (srvc *ReduceService) Reduce(ctx *ReduceRequestContext, _ *Void) error {
	// Initialize status
	if err := srvc.initStatus(ctx.ID); err != nil {
		return err
	}

	// Provision data
	srvc.dp = utils.MakeDataProvisioner(srvc.Host, ctx.Sources)
	paths := srvc.dp.ProvisionData()
	srvc.setStatus(REDUCING)

	handleError := func(err error) error {
		srvc.setStatus(ERROR)
		return err
	}

	// Open files
	its := []io.Reader{}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return handleError(err)
		}
		defer f.Close()
		its = append(its, f)
	}

	// Create key-values iterator
	kvIt, err := utils.MakeKeyValueIterator(its...)
	if err != nil {
		return handleError(err)
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
			return handleError(err)
		}

		// Print values. TODO: remove and store to file
		fmt.Printf("%s: %s\n", key, res)
	}

	srvc.setStatus(DONE)
	return nil
}

// Status returns the status of the Reduce service. If the service status is either
// DONE or ERROR, it is also reset to IDLE, so that the service can accept other
// reduce tasks. If the service status is PROVISIONING, a list of missing files, that
// is files for which download either failed or server information is missing, is
// returned to the caller
func (srvc *ReduceService) Status(ctx *ReduceRequestContext, s *ReduceServiceStatus) error {
	if srvc.id != ctx.ID {
		return errors.New("ReduceService: Status: invalid task ID")
	}

	s.Code = srvc.status.code
	if s.Code == PROVISIONING {
		s.MissingFiles = srvc.dp.MissingFiles()
	}

	if s.Code == ERROR || s.Code == DONE {
		srvc.setStatus(IDLE)
	}
	return nil
}

// AddSources updates the data sources consumed by the data provisioner. Only data
// sources already registered with the data provisioner can be updated
func (srvc *ReduceService) AddSources(ctx *ReduceRequestContext, _ *Void) error {
	if srvc.dp == nil {
		return nil
	}

	if srvc.id != ctx.ID {
		return errors.New("ReduceService: AddSources: invalid task ID")
	}

	for _, ds := range ctx.Sources {
		srvc.dp.AddSource(ds)
	}
	return nil
}
