package localstate

import (
	"sync"
	"time"

	"github.com/protoman92/gocontainer/pkg/gocollection"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

type localState struct {
	*Params
	mutex    *sync.RWMutex
	taskList gocollection.List
}

func (s *localState) firstIdleTask() (worker.Task, bool, error) {
	_, first, _ := s.taskList.GetFirstFunc(func(ix int, e interface{}) bool {
		if e, ok := e.(worker.Task); ok && e.IsIdle() {
			return true
		}

		return false
	})

	if e, ok := first.(worker.Task); ok {
		return e, true, nil
	}

	return worker.Task{}, false, nil
}

func (s *localState) updateOrAddTasks(tasks ...worker.Task) error {
	for ix := range tasks {
		task := tasks[ix]

		if ix, _, found := s.taskList.IndexOfFunc(func(ix int, e interface{}) bool {
			if e, ok := e.(worker.Task); ok && e.JobRequest.ID == task.JobRequest.ID {
				return true
			}

			return false
		}); found && ix >= 0 {
			s.taskList.SetAt(ix, task)
		} else {
			s.taskList.Add(task)
		}
	}

	return nil
}

func (s *localState) FirstIdleTask() (worker.Task, bool, error) {
	time.Sleep(s.Latency)
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.firstIdleTask()
}

func (s *localState) UpdateOrAddTasks(tasks ...worker.Task) error {
	time.Sleep(s.Latency)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.updateOrAddTasks(tasks...)
}

// NewLocalState returns a new LocalState.
func NewLocalState(params Params) masterstate.State {
	list := gocollection.NewDefaultSliceList()

	return &localState{
		Params:   &params,
		mutex:    &sync.RWMutex{},
		taskList: list,
	}
}
