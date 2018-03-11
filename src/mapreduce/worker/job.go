package worker

// AcceptJob accepts a job request.
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
			result.errCh <- nil
		}
	}
}
