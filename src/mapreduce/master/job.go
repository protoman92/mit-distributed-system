package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request *JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- &JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (m *master) loopJobRequest() {
	for {
		select {
		case <-m.shutdownCh:
			m.LogMan.Printf("%v: shutting down job queue.\n", m)
			return

		case result := <-m.delegate.jobRequestCh:
			m.LogMan.Printf("%v: received job %v\n", m, result.request)
			tasks := m.createTasks(result.request)
			err := m.State.RegisterTasks(tasks...)
			result.errCh <- err
		}
	}
}

func (m *master) createTasks(request *JobRequest) []*worker.Task {
	tasks := make([]*worker.Task, 0)

	for ix := range request.FilePaths {
		r := &worker.JobRequest{
			FilePath:      request.FilePaths[ix],
			MapFuncName:   request.MapFuncName,
			MapOpCount:    request.MapOpCount,
			ReduceOpCount: request.ReduceOpCount,
			Type:          request.Type,
		}

		worker.CheckJobRequest(r)

		task := &worker.Task{
			JobRequest: r,
			Status:     mrutil.Idle,
			Worker:     mrutil.UnassignedWorker,
		}

		worker.CheckTask(task)
		tasks = append(tasks, task)
	}

	return tasks
}
