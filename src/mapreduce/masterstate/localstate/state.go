package localstate

import (
	"github.com/protoman92/gocontainer/pkg/gocollection"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

type localState struct {
	taskList gocollection.List
}

// NewLocalState returns a new LocalState.
func NewLocalState() masterstate.State {
	list := gocollection.NewDefaultSliceList()
	concurrent := gocollection.NewLockConcurrentList(list)
	return &localState{taskList: concurrent}
}

func (s *localState) IdleTasks() ([]*worker.Task, error) {
	elements := s.taskList.GetAllFunc(func(e interface{}) bool {
		if e, ok := e.(*worker.Task); ok && e.Status == mrutil.Idle {
			return true
		}

		return false
	})

	tasks := make([]*worker.Task, 0)

	for ix := range elements {
		if e, ok := elements[ix].(*worker.Task); ok {
			tasks = append(tasks, e)
		}
	}

	return tasks, nil
}

func (s *localState) RegisterTasks(tasks ...*worker.Task) error {
	elements := make([]interface{}, len(tasks))

	for ix := range tasks {
		elements[ix] = tasks[ix]
	}

	s.taskList.AddAll(elements...)
	return nil
}
