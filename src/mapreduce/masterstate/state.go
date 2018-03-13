package masterstate

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// StateJobMap stores the jobs and their current statuses.
type StateJobMap = map[job.WorkerJob]mrutil.JobStatus

// State represents a master's mutable state, in which it keeps job information.
// A State object abstracts away key-value get/set implementations, so we can
// have local State or remote State with database access.
type State interface {
	FirstIdleJob() (job.WorkerJob, bool, error)
	UpdateOrAddJobs(jobs StateJobMap) error
}
