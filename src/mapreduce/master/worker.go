package master

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"

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
			var task worker.Task
			var found bool

			firstIdle := func() error {
				t, f, err := m.State.FirstIdleTask()
				task = t
				found = f
				return err
			}

			// If no idle task is found, requeue the worker; repeat until there is
			// an idle task.
			if err := m.RPCParams.RetryWithDelay(firstIdle)(); !found {
				if err != nil {
					m.errCh <- err
				}

				workerRequeueCh = m.workerQueueCh
				break
			}

			assignWork := func() error {
				return m.assignWork(w, task)
			}

			// If the master is unable to assign work to this worker (due to worker
			// failure or RPC failure), requeue this worker and repeat the process.
			if err := m.RPCParams.RetryWithDelay(assignWork)(); err != nil {
				m.errCh <- err
				workerRequeueCh = m.workerQueueCh
				break
			}

			cloned := task.Clone()
			cloned.Status = mrutil.InProgress
			cloned.Worker = w

			updateTask := func() error {
				return m.State.UpdateOrAddTasks(cloned)
			}

			// If we fail to update/add the task (to in-progress), do not requeue the
			// worker, because it may be busy processing the task	we just assigned.
			// Unfortunately, we would not be able to use this worker's results to
			// continue the process, because we are retrying the sequence until there
			// are no errors (which means this task may be assigned to another worker).
			if err := m.RPCParams.RetryWithDelay(updateTask)(); err != nil {
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

func (m *master) assignWork(w string, task worker.Task) error {
	reply := &worker.JobReply{}

	callParams := rpcutil.CallParams{
		Args:    task.JobRequest,
		Method:  m.WorkerAcceptJobMethod,
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	return m.rpcHandler.Call(callParams)
}
