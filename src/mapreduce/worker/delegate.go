package worker

// WkDelegate represents a RPC Worker's exported delegate that transmits events
// back to said worker.
type WkDelegate struct {
	jobRequestCh chan<- *jobRequest
	shutdownCh   chan<- interface{}
}

// DoWork accepts a job via RPC, performs it and sends back a reply.
func (d *WkDelegate) DoWork(args *JobParams, reply *JobReply) error {
	errCh := make(chan error, 0)
	d.jobRequestCh <- &jobRequest{details: args, errCh: errCh}
	return <-errCh
}

// ShutDown shuts down a worker.
func (d *WkDelegate) ShutDown(args *ShutdownParams, reply *ShutdownReply) error {
	d.shutdownCh <- true
	return nil
}
