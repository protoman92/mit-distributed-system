package master

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// AcceptJob accepts a job request.
func (d *MstDelegate) AcceptJob(request JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobRequestCh <- JobCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (m *master) loopJobRequest() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.delegate.jobRequestCh:
			m.LogMan.Printf("%v: received job %v\n", m, result.request)
			tasks := m.createTasks(result.request)

			registerTask := func() error {
				return m.State.UpdateOrAddTasks(tasks...)
			}

			err := m.RPCParams.RetryWithDelay(registerTask)()
			result.errCh <- err
		}
	}
}

func (m *master) createTasks(request JobRequest) []worker.Task {
	tasks := make([]worker.Task, 0)

	for ix := range request.FilePaths {
		path := request.FilePaths[ix]
		id := m.formatJobID(path, request.Type)

		r := worker.JobRequest{
			FilePath:      path,
			ID:            id,
			MapFuncName:   request.MapFuncName,
			MapOpCount:    request.MapOpCount,
			ReduceOpCount: request.ReduceOpCount,
			Type:          request.Type,
		}

		worker.CheckJobRequest(r)

		task := worker.Task{
			JobRequest: r,
			Status:     mrutil.Idle,
			Worker:     mrutil.UnassignedWorker,
		}

		worker.CheckTask(task)
		tasks = append(tasks, task)
	}

	return tasks
}

func (m *master) formatJobID(path string, tType mrutil.TaskType) string {
	return fmt.Sprintf("%s-%s", tType, path)
}
