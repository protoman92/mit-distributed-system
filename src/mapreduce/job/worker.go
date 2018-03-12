package job

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// WorkerJobRequest represents a Map/Reduce job request.
type WorkerJobRequest struct {
	FilePath       string
	JobNumber      uint
	MapFuncName    mrutil.MapFuncName
	MapOpCount     uint
	ReduceFuncName mrutil.ReduceFuncName
	ReduceOpCount  uint
	Type           mrutil.JobType
	Worker         string
}

// CheckWorkerJobRequest checks the validity of a JobRequest.
func CheckWorkerJobRequest(r WorkerJobRequest) {
	if r.FilePath == "" ||
		r.MapFuncName == "" ||
		r.MapOpCount == 0 ||
		r.ReduceFuncName == "" ||
		r.ReduceOpCount == 0 ||
		r.Type == mrutil.JobType(0) ||
		r.Worker == "" {
		panic("Invalid parameters")
	}
}

// Clone clones a job request.
func (r WorkerJobRequest) Clone() WorkerJobRequest {
	return WorkerJobRequest{
		FilePath:       r.FilePath,
		JobNumber:      r.JobNumber,
		MapFuncName:    r.MapFuncName,
		MapOpCount:     r.MapOpCount,
		ReduceFuncName: r.ReduceFuncName,
		ReduceOpCount:  r.ReduceOpCount,
		Type:           r.Type,
	}
}

// Equals checks whether two job requests are the same.
func (r WorkerJobRequest) Equals(r1 WorkerJobRequest) bool {
	return r.FilePath == r1.FilePath &&
		r.JobNumber == r1.JobNumber &&
		r.Type == r1.Type
}

// UID returns a unique ID for the current job request.
func (r WorkerJobRequest) UID() string {
	return fmt.Sprintf("%s-%d-%s", r.Type, r.JobNumber, r.FilePath)
}
