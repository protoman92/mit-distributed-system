package master

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

func (m *master) loopWorker() {
	resetCh := make(chan interface{}, 1)
	workerQueueCh := m.workerQueueCh
	var w string
	var workerRequeueCh chan<- string

	for {
		select {
		case <-m.shutdownCh:
			return

		case w = <-workerQueueCh:
			m.LogMan.Printf("%v: worker %s is ready to work.\n", m, w)
			workerQueueCh = nil
			var job job.WorkerJobRequest
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

			assignWork := func() error {
				return m.assignWork(w, job)
			}

			// If the master is unable to assign work to this worker (due to worker
			// failure or RPC failure), requeue this worker and repeat the process.
			if err := m.RPCParams.RetryWithDelay(assignWork)(); err != nil {
				m.errCh <- err
				workerRequeueCh = m.workerQueueCh
				break
			}

			cloned := job.Clone()
			cloned.Worker = w

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

func (m *master) assignWork(w string, job job.WorkerJobRequest) error {
	reply := &worker.JobReply{}

	callParams := rpcutil.CallParams{
		Args:    job,
		Method:  m.WorkerAcceptJobMethod,
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	return m.rpcHandler.Call(callParams)
}
