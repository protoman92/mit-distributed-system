package job

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// MasterJob represents job request sent to a master.
type MasterJob struct {
	FilePaths      []string
	MapFuncName    mrutil.MapFuncName
	MapOpCount     uint
	ReduceFuncName mrutil.ReduceFuncName
	ReduceOpCount  uint
	Type           mrutil.JobType
}

// CheckMasterJob checks the validity of a JobRequest.
func CheckMasterJob(r MasterJob) {
	if r.FilePaths == nil ||
		r.MapFuncName == "" ||
		r.MapOpCount == 0 ||
		r.ReduceFuncName == "" ||
		r.ReduceOpCount == 0 ||
		r.Type == 0 {
		panic("Invalid parameters")
	}
}

// JobCount returns a total number of jobs.
func (r MasterJob) JobCount() uint {
	return uint(len(r.FilePaths)) * r.MapOpCount
}

// MapJobs creates Map jobs.
func (r MasterJob) MapJobs() []WorkerJob {
	requests := make([]WorkerJob, 0)

	for ix := range r.FilePaths {
		path := r.FilePaths[ix]

		for jx := 0; jx < int(r.MapOpCount); jx++ {
			request := WorkerJob{
				File:           path,
				MapJobNumber:   uint(jx),
				MapOpCount:     r.MapOpCount,
				MapFuncName:    r.MapFuncName,
				ReduceFuncName: r.ReduceFuncName,
				ReduceOpCount:  r.ReduceOpCount,
				RemoteFileAddr: mrutil.UnassignedWorker,
				Type:           r.Type,
				Worker:         mrutil.UnassignedWorker,
			}

			requests = append(requests, request)
		}
	}

	return requests
}
