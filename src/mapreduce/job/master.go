package job

import (
	"fmt"
	"strings"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// MasterJobRequest represents job request sent to a master.
type MasterJobRequest struct {
	FilePaths      []string
	MapFuncName    mrutil.MapFuncName
	MapOpCount     uint
	ReduceFuncName mrutil.ReduceFuncName
	ReduceOpCount  uint
	Type           mrutil.JobType
}

func (r MasterJobRequest) String() string {
	return fmt.Sprintf("Job request:\n %s", strings.Join(r.FilePaths, "\n"))
}

// CheckMasterJobRequest checks the validity of a JobRequest.
func CheckMasterJobRequest(r MasterJobRequest) {
	if r.FilePaths == nil ||
		r.MapFuncName == "" ||
		r.MapOpCount == 0 ||
		r.ReduceFuncName == "" ||
		r.ReduceOpCount == 0 ||
		r.Type == 0 {
		panic("Invalid parameters")
	}
}

// WorkerJobs creates worker jobs.
func (r MasterJobRequest) WorkerJobs() []WorkerJobRequest {
	requests := make([]WorkerJobRequest, 0)

	for ix := range r.FilePaths {
		path := r.FilePaths[ix]

		request := WorkerJobRequest{
			FilePath:       path,
			JobNumber:      uint(ix),
			MapFuncName:    r.MapFuncName,
			MapOpCount:     r.MapOpCount,
			ReduceFuncName: r.ReduceFuncName,
			ReduceOpCount:  r.ReduceOpCount,
			Type:           r.Type,
			Worker:         mrutil.UnassignedWorker,
		}

		CheckWorkerJobRequest(request)
		requests = append(requests, request)
	}

	return requests
}
