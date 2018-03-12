package worker

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

// Worker represents a MapReduce worker.
type Worker interface {
	ErrorChannel() <-chan error
}

type worker struct {
	*Params
	delegate   *WkDelegate
	rpcHandler rpchandler.Handler
	capacityCh chan interface{}
	errCh      chan error
	jobQueueCh chan job.WorkerJobRequest
	shutdownCh chan interface{}
}

// NewWorker returns a new Worker.
func NewWorker(params Params) Worker {
	checked := checkParams(&params)
	delegate := newDelegate()
	checkDelegate(delegate)

	w := &worker{
		Params:     checked,
		delegate:   delegate,
		rpcHandler: rpchandler.NewHandler(checked.RPCParams, delegate),
		capacityCh: make(chan interface{}, params.JobCapacity),
		errCh:      make(chan error, 0),
		jobQueueCh: make(chan job.WorkerJobRequest),
		shutdownCh: make(chan interface{}, 0),
	}

	checkWorker(w)
	go w.loopError()
	go w.loopJobReceipt()
	go w.loopJobRequest()
	go w.loopRegister()
	go w.loopShutdown()
	return w
}
