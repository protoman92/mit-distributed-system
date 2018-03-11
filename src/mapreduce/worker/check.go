package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// CheckJobRequest checks the validity of a JobRequest.
func CheckJobRequest(r *JobRequest) {
	if r.FilePath == "" || r.JobNumber == 0 || r.Type == mrutil.TaskType(0) {
		panic("Invalid parameters")
	}
}

// CheckTask checks the validity of a task.
func CheckTask(t *Task) {
	if t.Status == mrutil.TaskStatus(0) || t.Worker == "" {
		panic("Invalid parameters")
	}
}

func checkParams(params *Params) *Params {
	if params.MasterRegisterMethod == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkWorker(worker *worker) {
	if worker.errCh == nil || worker.shutdownCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(delegate *WkDelegate) {
	if delegate.jobCh == nil {
		panic("Invalid setup")
	}
}
