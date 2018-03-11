package worker

// WkDelegate represents a RPC Delegate for a worker.
type WkDelegate struct {
	jobCh chan *JobCallResult
}

func newDelegate() *WkDelegate {
	return &WkDelegate{
		jobCh: make(chan *JobCallResult, 0),
	}
}
