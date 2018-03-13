package localstate

import (
	"sync"
	"time"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/masterstate"
)

type localState struct {
	*Params
	mutex  *sync.RWMutex
	jobs   []job.WorkerJob
	status map[string]mrutil.JobStatus
}

func (s *localState) firstIdleJob() (job.WorkerJob, bool, error) {
	for ix := range s.jobs {
		if s.status[s.jobs[ix].UID()] == mrutil.Idle {
			return s.jobs[ix], true, nil
		}
	}

	return job.WorkerJob{}, false, nil
}

func (s *localState) FirstIdleJob() (job.WorkerJob, bool, error) {
	time.Sleep(s.Latency)
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.firstIdleJob()
}

func (s *localState) updateOrAddJobs(jobs masterstate.StateJobMap) error {
	for job := range jobs {
		if _, found := s.status[job.UID()]; !found {
			s.jobs = append(s.jobs, job)
		}

		s.status[job.UID()] = jobs[job]
	}

	return nil
}

func (s *localState) UpdateOrAddJobs(jobs masterstate.StateJobMap) error {
	time.Sleep(s.Latency)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.updateOrAddJobs(jobs)
}

// NewLocalState returns a new LocalState.
func NewLocalState(params Params) masterstate.State {
	return &localState{
		Params: &params,
		mutex:  &sync.RWMutex{},
		jobs:   make([]job.WorkerJob, 0),
		status: make(map[string]mrutil.JobStatus),
	}
}
