package job

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
)

// WorkerJob represents a Map/Reduce job request.
type WorkerJob struct {
	File           string
	MapFuncName    mrutil.MapFuncName
	MapOpCount     uint
	ReduceFuncName mrutil.ReduceFuncName
	ReduceOpCount  uint

	// These properties can be changed.
	JobNumber         uint
	RemoteFileAddress string
	Type              mrutil.JobType
	Worker            string
}

// CheckWorkerJob checks the validity of a JobRequest.
func CheckWorkerJob(r WorkerJob) {
	if r.File == "" ||
		r.MapFuncName == "" ||
		r.MapOpCount == 0 ||
		r.ReduceFuncName == "" ||
		r.ReduceOpCount == 0 ||
		r.RemoteFileAddress == "" ||
		r.Type == mrutil.JobType(0) ||
		r.Worker == "" {
		panic("Invalid parameters")
	}
}

// Clone clones the current args.
func (r WorkerJob) Clone() WorkerJob {
	return r
}

// Equals checks equality.
func (r WorkerJob) Equals(r1 WorkerJob) bool {
	return r.File == r1.File && r.JobNumber == r1.JobNumber && r.Type == r1.Type
}

// UID returns a unique ID for the current job request.
func (r WorkerJob) UID() string {
	return fmt.Sprintf("%s-%d-%s", r.Type, r.JobNumber, r.File)
}
