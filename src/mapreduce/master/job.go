package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request job.MasterJob, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

// CompleteJob completes a request and initiates a new request if required.
func (d *MstDelegate) CompleteJob(request job.WorkerJob, reply *worker.JobReply) error {
	job.CheckWorkerJob(request)
	resultCh := make(chan error, 0)
	d.jobCompleteCh <- worker.JobCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) CompletionChannel() <-chan job.WorkerJob {
	return m.completionCh
}

func (m *master) loopJobReceipt() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.Delegate.jobRequestCh:
			m.LogMan.Printf("%v: received job %v\n", m, result.request)
			requests := result.request.MapJobs()
			update := make(masterstate.StateJobMap, 0)

			for ix := range requests {
				update[requests[ix]] = mrutil.Idle
			}

			registerJobs := func() error {
				return m.State.UpdateOrAddJobs(update)
			}

			err := m.RPCParams.RetryWithDelay(registerJobs)()
			result.errCh <- err
		}
	}
}

func (m *master) loopJobAssignment() {
	resetCh := make(chan interface{}, 1)
	workerQueueCh := m.workerQueueCh
	var w string
	var workerRequeueCh chan<- string

	for {
		select {
		case <-m.shutdownCh:
			return

		case w = <-workerQueueCh:
			workerQueueCh = nil
			var job job.WorkerJob
			var found bool

			firstIdle := func() error {
				t, f, err := m.State.FirstIdleJob()
				job = t
				found = f
				return err
			}

			// If no idle job is found, requeue the worker; repeat until there is
			// an idle job.
			if err := m.RPCParams.RetryWithDelay(firstIdle)(); !found {
				if err != nil {
					m.errCh <- err
				}

				workerRequeueCh = m.workerQueueCh
				break
			}

			cloned := job.Clone()
			cloned.Worker = w

			assignWork := func() error {
				return m.assignWork(w, cloned)
			}

			// If the master is unable to assign work to this worker (due to worker
			// failure or RPC failure), requeue this worker and repeat the process.
			if err := m.RPCParams.RetryWithDelay(assignWork)(); err != nil {
				m.errCh <- err
				workerRequeueCh = m.workerQueueCh
				break
			}

			updateJob := func() error {
				update := masterstate.StateJobMap{cloned: mrutil.InProgress}
				return m.State.UpdateOrAddJobs(update)
			}

			// If we fail to update/add the job (to in-progress), do not requeue the
			// worker, because it may be busy processing the job	we just assigned.
			// Unfortunately, we would not be able to use this worker's results to
			// continue the process, because we are retrying the sequence until there
			// are no errors (which means this job may be assigned to another worker).
			if err := m.RPCParams.RetryWithDelay(updateJob)(); err != nil {
				m.errCh <- err
			}

			resetCh <- true

		case workerRequeueCh <- w:
			workerRequeueCh = nil
			resetCh <- true

		case <-resetCh:
			workerQueueCh = m.workerQueueCh
		}
	}
}

func (m *master) loopJobCompletion() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.Delegate.jobCompleteCh:
			handleCompletion := func() error {
				return m.handleJobCompletion(result.Request)
			}

			if err := m.RPCParams.RetryWithDelay(handleCompletion)(); err != nil {
				result.ErrCh <- err
			} else {
				result.ErrCh <- nil

				if result.Request.Type == mrutil.Reduce {
					m.completionCh <- result.Request
				}
			}
		}
	}
}

func (m *master) assignWork(w string, job job.WorkerJob) error {
	reply := &worker.JobReply{}

	callParams := rpcutil.CallParams{
		Args:    job,
		Method:  m.WorkerAcceptJobMethod,
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	return m.RPCHandler.Call(callParams)
}

func (m *master) handleJobCompletion(r job.WorkerJob) error {
	update := make(masterstate.StateJobMap, 0)
	update[r] = mrutil.Completed

	switch r.Type {
	case mrutil.Map:
		for i := 0; i < int(r.ReduceOpCount); i++ {
			reduceR := r.Clone()
			reduceR.JobNumber = uint(i)
			reduceR.Type = mrutil.Reduce
			reduceR.RemoteFileAddr = r.Worker
			reduceR.Worker = mrutil.UnassignedWorker
			update[reduceR] = mrutil.Idle
		}

		return m.State.UpdateOrAddJobs(update)

	case mrutil.Reduce:
		return m.State.UpdateOrAddJobs(update)
	}

	panic("Unsupported operation")
}
