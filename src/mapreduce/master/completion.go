package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// CompleteJob completes a request and initiates a new request if required.
func (d *MstDelegate) CompleteJob(request job.WorkerJobRequest, reply *worker.JobReply) error {
	resultCh := make(chan error, 0)
	d.jobCompleteCh <- worker.JobCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) loopJobCompletion() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.delegate.jobCompleteCh:
			handleCompletion := func() error {
				return m.handleJobCompletion(result.Request)
			}

			err := m.RPCParams.RetryWithDelay(handleCompletion)()
			result.ErrCh <- err
		}
	}
}

func (m *master) handleJobCompletion(t job.WorkerJobRequest) error {
	update := make(masterstate.StateJobMap, 0)
	update[t] = mrutil.Completed

	switch t.Type {
	case mrutil.Map:
		request := t.Clone()
		request.Type = mrutil.Reduce
		request.Worker = mrutil.UnassignedWorker
		update[request] = mrutil.Idle
		return m.State.UpdateOrAddJobs(update)

	default:
		panic("Unsupported operation")
	}
}
