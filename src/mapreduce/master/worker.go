package master

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

func (m *master) loopWorker() {
	resetCh := make(chan interface{}, 1)
	idleTaskCh := m.State.IdleTaskChannel()
	var task *worker.Task
	var workerQueueCh <-chan string
	var workerRequeueCh chan<- string
	var w string

	for {
		select {
		case <-m.shutdownCh:
			m.LogMan.Printf("%v: shutting down worker queue.\n", m)
			return

		case task = <-idleTaskCh:
			idleTaskCh = nil
			workerQueueCh = m.workerQueueCh

		case w = <-workerQueueCh:
			workerQueueCh = nil
			m.LogMan.Printf("%v: worker %s is ready to work.\n", m, w)

			if err := m.assignWork(w, task); err != nil {
				m.errCh <- err
				workerRequeueCh = m.workerQueueCh
			} else {
				task.Status = mrutil.InProgress
				task.Worker = w

				// If we fail to update/add the task (to in-progress), refresh state
				// to redeposit idle tasks.
				if err := m.State.UpdateOrAddTasks(task); err != nil {
					task.Status = mrutil.Idle
					task.Worker = mrutil.UnassignedWorker
					m.State.NotifyIdleTasks(task)
					resetCh <- true
				}
			}

		case workerRequeueCh <- w:
			resetCh <- true

		case <-resetCh:
			idleTaskCh = m.State.IdleTaskChannel()
		}
	}
}

func (m *master) assignWork(w string, task *worker.Task) error {
	reply := &worker.JobReply{}

	callParams := rpcutil.CallParams{
		Args:    task.JobRequest,
		Method:  "WkDelegate.AcceptJob",
		Network: m.RPCParams.Network,
		Reply:   reply,
		Target:  w,
	}

	return rpcutil.Call(callParams)
}
