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
	mutex    *sync.RWMutex
	jobs     []job.WorkerJob
	tempIdle map[string]bool
	status   map[string]mrutil.JobStatus
}

func (s *localState) checkAllJobsCompleted(jobCount uint) (bool, error) {
	current := 0

	for ix := range s.jobs {
		job := s.jobs[ix]

		if job.Type == mrutil.Reduce && s.status[job.UID()] == mrutil.Completed {
			current++
		}
	}

	return uint(current) == jobCount, nil
}

func (s *localState) CheckAllJobsCompleted(jobCount uint) (bool, error) {
	time.Sleep(s.Latency * 2)
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.checkAllJobsCompleted(jobCount)
}

func (s *localState) firstIdleJob() (job.WorkerJob, bool, error) {
	for ix := range s.jobs {
		job := s.jobs[ix]
		uid := job.UID()

		if s.status[uid] == mrutil.Idle && !s.tempIdle[uid] {
			s.tempIdle[uid] = true
			return s.jobs[ix], true, nil
		}
	}

	return job.WorkerJob{}, false, nil
}

func (s *localState) FirstIdleJob() (job.WorkerJob, bool, error) {
	time.Sleep(s.Latency)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.firstIdleJob()
}

func (s *localState) updateOrAddJobs(jobs masterstate.StateJobMap) error {
	for job := range jobs {
		if _, found := s.status[job.UID()]; !found {
			s.jobs = append(s.jobs, job)
		}

		uid := job.UID()
		s.status[uid] = jobs[job]
		delete(s.tempIdle, uid)
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
		Params:   &params,
		mutex:    &sync.RWMutex{},
		jobs:     make([]job.WorkerJob, 0),
		tempIdle: make(map[string]bool, 0),
		status:   make(map[string]mrutil.JobStatus),
	}
}
