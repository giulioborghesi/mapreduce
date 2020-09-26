package workers

// Status is a RPC endpoint used by the master to send heartbeat messages to
// workers
func (srvc *MapReduceService) Status(_ Void, _ *Void) error {
	return nil
}
