package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
)

// Worker represents a MapReduce worker.
type Worker interface {
	ErrorChannel() <-chan error
}

type worker struct {
	*Params
	capacityCh chan interface{}
	errCh      chan error
	jobQueueCh chan job.WorkerJob
	shutdownCh chan interface{}
}

// NewWorker returns a new Worker.
func NewWorker(params Params) Worker {
	checked := checkParams(&params)

	w := &worker{
		Params:     checked,
		capacityCh: make(chan interface{}, params.JobCapacity),
		errCh:      make(chan error, 0),
		jobQueueCh: make(chan job.WorkerJob),
		shutdownCh: make(chan interface{}, 0),
	}

	checkWorker(w)
	go w.loopError()
	go w.loopFileAccess()
	go w.loopJobReceipt()
	go w.loopJobRequest()
	go w.loopRegister()
	go w.loopShutdown()
	return w
}
