package master

// MstDelegate represents a Master's RPC delegate that can be exported and
// relays requests to the master.
type MstDelegate struct {
	jobRequestCh chan<- *JobCallResult
	shutdownCh   chan<- *ShutdownCallResult
}

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request *JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- &JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

// Shutdown accepts a shutdown request.
func (d *MstDelegate) Shutdown(request *ShutdownRequest, reply *ShutdownReply) error {
	resultCh := make(chan error, 0)
	d.shutdownCh <- &ShutdownCallResult{request: request, errCh: resultCh}
	return <-resultCh
}
