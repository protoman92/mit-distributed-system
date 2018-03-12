package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// CompleteJob completes a job request and initiates a new job if required.
func (d *MstDelegate) CompleteJob(task worker.Task, reply *worker.TaskReply) error {
	resultCh := make(chan error, 0)
	d.jobCompleteCh <- worker.TaskCallResult{Task: task, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) loopJobCompletion() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.delegate.jobCompleteCh:
			handleCompletion := func() error {
				return m.handleJobCompletion(result.Task)
			}

			err := m.RPCParams.RetryWithDelay(handleCompletion)()
			result.ErrCh <- err
		}
	}
}

func (m *master) handleJobCompletion(t worker.Task) error {
	switch t.Type {
	case mrutil.Map:
		newID := m.formatJobID(t.FilePath, mrutil.Reduce)
		request := t.JobRequest.Clone()
		request.ID = newID
		request.Type = mrutil.Reduce

		newTask := worker.Task{
			JobRequest: request,
			Status:     mrutil.Idle,
			Worker:     mrutil.UnassignedWorker,
		}

		return m.State.UpdateOrAddTasks(t, newTask)

	default:
		panic("Unsupported operation")
	}
}
