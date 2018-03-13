package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// CompleteJob completes a request and initiates a new request if required.
func (d *MstDelegate) CompleteJob(request job.WorkerJob, reply *worker.JobReply) error {
	job.CheckWorkerJob(request)
	resultCh := make(chan error, 0)
	d.jobCompleteCh <- worker.JobCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) AllCompletedChannel() <-chan interface{} {
	return m.allCompletedCh
}

func (m *master) loopJobComplete() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case request := <-m.jobCompleteCh:
			go func() {
				handleCompletion := func() error {
					return m.handleJobCompletion(request)
				}

				m.RPCParams.RetryWithDelay(handleCompletion)()
			}()
		}
	}
}

func (m *master) loopJobCompleteNotification() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.Delegate.jobCompleteCh:
			m.jobCompleteCh <- result.Request
			result.ErrCh <- nil
		}
	}
}

func (m *master) loopAllCompletion() {
	for {
		select {
		case <-m.shutdownCh:
			return

		default:
			completed, err := m.State.CheckAllJobsCompleted(m.JobRequest.JobCount())

			if err == nil && completed {
				m.allCompletedCh <- true
				return
			}
		}
	}
}

func (m *master) handleJobCompletion(r job.WorkerJob) error {
	update := make(masterstate.StateJobMap, 0)
	update[r] = mrutil.Completed

	switch r.Type {
	case mrutil.Map:
		reduceR := r.Clone()
		reduceR.Type = mrutil.Reduce
		reduceR.RemoteFileAddr = r.Worker
		reduceR.Worker = mrutil.UnassignedWorker
		update[reduceR] = mrutil.Idle
		return m.State.UpdateOrAddJobs(update)

	case mrutil.Reduce:
		return m.State.UpdateOrAddJobs(update)
	}

	panic("Unsupported operation")
}
