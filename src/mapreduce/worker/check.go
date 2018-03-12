package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// CheckJobRequest checks the validity of a JobRequest.
func CheckJobRequest(r JobRequest) {
	if r.FilePath == "" ||
		r.ID == "" ||
		r.MapFuncName == "" ||
		r.MapOpCount == 0 ||
		r.ReduceOpCount == 0 ||
		r.Type == mrutil.TaskType(0) {
		panic("Invalid parameters")
	}
}

// CheckTask checks the validity of a task.
func CheckTask(t Task) {
	if t.Status == mrutil.TaskStatus(0) || t.Worker == "" {
		panic("Invalid parameters")
	}
}

func checkParams(params *Params) *Params {
	if params.FileAccessor == nil ||
		params.JobCapacity == 0 ||
		params.MasterAddress == "" ||
		params.MasterCompleteJobMethod == "" ||
		params.MasterRegisterMethod == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{Log: true})
	}

	return params
}

func checkWorker(worker *worker) {
	if worker.capacityCh == nil ||
		worker.errCh == nil ||
		worker.jobQueueCh == nil ||
		worker.shutdownCh == nil {
		panic("Invalid setup")
	}
}

func checkDelegate(delegate *WkDelegate) {
	if delegate.jobCh == nil {
		panic("Invalid setup")
	}
}
