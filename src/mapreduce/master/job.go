package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

func (m *master) updateWorkerJobs() {
	requests := m.JobRequest.MapJobs()
	update := make(masterstate.StateJobMap, 0)

	for ix := range requests {
		update[requests[ix]] = mrutil.Idle
	}

	registerJobs := func() error {
		return m.State.UpdateOrAddJobs(update)
	}

	// If this fails, there's no point continuing.
	if err := m.RPCParams.RetryWithDelay(registerJobs)(); err != nil {
		panic(err)
	}
}

func (m *master) loopJobAssignment() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case w := <-m.workerQueueCh:
			go func(w string) {
				if requeue := m.searchIdleJobs(w); requeue {
					m.workerQueueCh <- w
				}
			}(w)
		}
	}
}

func (m *master) searchIdleJobs(w string) (requeue bool) {
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
	if err := m.RPCParams.RetryWithDelay(firstIdle)(); err != nil || !found {
		requeue = true
		return
	}

	cloned := job.Clone()
	cloned.Worker = w

	assignWork := func() error {
		return m.assignWork(w, cloned)
	}

	// If the master is unable to assign work to this worker (due to worker
	// failure or RPC failure), requeue this worker and repeat the process.
	if err := m.RPCParams.RetryWithDelay(assignWork)(); err != nil {
		requeue = true
		return
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
	m.RPCParams.RetryWithDelay(updateJob)()
	return
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
