package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request job.MasterJobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (r *master) loopJobRequest() {
	for {
		select {
		case <-r.shutdownCh:
			return

		case result := <-r.delegate.jobRequestCh:
			r.LogMan.Printf("%v: received job %v\n", r, result.request)
			requests := result.request.WorkerJobs()
			update := make(masterstate.StateJobMap, 0)

			for ix := range requests {
				update[requests[ix]] = mrutil.Idle
			}

			registerJobs := func() error {
				return r.State.UpdateOrAddJobs(update)
			}

			err := r.RPCParams.RetryWithDelay(registerJobs)()
			result.errCh <- err
		}
	}
}
