package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// AcceptJob accepts a job request. If this worker machine does not have the
// file specified by the job request, we should return an error.
func (d *WkDelegate) AcceptJob(request *JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobCh <- &JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (w *worker) loopJobRequest() {
	for {
		select {
		case <-w.shutdownCh:
			w.LogMan.Printf("%v: shutting down job queue.\n", w)
			return

		case result := <-w.delegate.jobCh:
			w.LogMan.Printf("%v: received job request %v\n", w, result.request)
			err := w.handleJobRequest(result.request)
			result.errCh <- err
		}
	}
}

func (w *worker) handleJobRequest(r *JobRequest) error {
	switch r.Type {
	case mrutil.Map:
		return w.doMap(r)

	default:
		panic("Invalid type")
	}
}
