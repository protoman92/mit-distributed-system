package localstate

import (
	"sync"

	"github.com/protoman92/gocontainer/pkg/gocollection"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

type localState struct {
	mutex    *sync.RWMutex
	taskList gocollection.List
	idleCh   chan *worker.Task
}

// NewLocalState returns a new LocalState.
func NewLocalState() masterstate.State {
	list := gocollection.NewDefaultSliceList()

	return &localState{
		mutex:    &sync.RWMutex{},
		taskList: list,
		idleCh:   make(chan *worker.Task),
	}
}

func (s *localState) IdleTaskChannel() chan *worker.Task {
	return s.idleCh
}

func (s *localState) NotifyIdleTasks(tasks ...*worker.Task) {
	for ix := range tasks {
		go func(task *worker.Task) {
			s.idleCh <- task
		}(tasks[ix])
	}
}

func (s *localState) Refresh() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.refresh()
}

func (s *localState) RegisterTasks(tasks ...*worker.Task) error {
	elements := make([]interface{}, len(tasks))

	for ix := range tasks {
		task := tasks[ix]
		elements[ix] = task

		if task.IsIdle() {
			go func() {
				s.idleCh <- task
			}()
		}
	}

	s.mutex.Lock()
	s.taskList.AddAll(elements...)
	s.refresh()
	s.mutex.Unlock()
	return nil
}

func (s *localState) UpdateOrAddTasks(tasks ...*worker.Task) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for ix := range tasks {
		task := tasks[ix]

		if ix, _, found := s.taskList.IndexOfFunc(func(ix int, e interface{}) bool {
			if e, ok := e.(*worker.Task); ok && e.JobRequest == task.JobRequest {
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

func (s *localState) refresh() {
	tasks := s.taskList.GetAllFunc(func(e interface{}) bool {
		if e, ok := e.(*worker.Task); ok && e.IsIdle() {
			return true
		}

		return false
	})

	for ix := range tasks {
		go func(task interface{}) {
			if task, ok := task.(*worker.Task); ok {
				s.idleCh <- task
			}
		}(tasks[ix])
	}
}
