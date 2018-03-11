package master

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request *JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- &JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (m *master) loopJobRequest() {
	for {
		select {
		case result := <-m.delegate.jobRequestCh:
			m.LogMan.Printf("%v: received job %v\n", m, result.request)
			result.errCh <- nil
		}
	}
}
