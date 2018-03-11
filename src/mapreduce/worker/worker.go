package worker

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

// Worker represents a MapReduce worker.
type Worker interface {
	ErrorChannel() <-chan error
}

type worker struct {
	*Params
	rpcHandler rpchandler.Handler
	errCh      chan error
}

// NewWorker returns a new Worker.
func NewWorker(params Params) Worker {
	checked := checkParams(&params)
	delegate := &WkDelegate{}
	checkDelegate(delegate)

	w := &worker{
		Params:     checked,
		rpcHandler: rpchandler.NewHandler(checked.RPCParams, delegate),
		errCh:      make(chan error, 0),
	}

	checkWorker(w)
	go w.registerWithMaster()
	go w.loopError()
	go w.loopShutdown()
	return w
}
