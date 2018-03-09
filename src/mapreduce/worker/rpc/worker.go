package rpc

import (
	"net"
	"sync"

	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// This is used internally by a worker to receive jobs and return errors if
// present.
type jobRequest struct {
	details *JobParams
	errCh   chan error
}

// Params represents the required parameters to build a RPC worker.
type Params struct {
	Address              string
	MasterAddress        string
	MasterRegisterMethod string
	Network              string
}

func checkParams(params *Params) *Params {
	if params.Address == "" ||
		params.MasterAddress == "" ||
		params.MasterRegisterMethod == "" ||
		params.Network == "" {
		panic("Invalid parameters")
	}

	return params
}

// This communicates with the master via RPC.
type servant struct {
	*Params
	delegate           *WkDelegate
	mutex              sync.RWMutex
	listener           net.Listener
	jobRequestCh       chan *jobRequest
	registerShutdownCh chan interface{}
	shutdownCh         chan interface{}
	workShutdownCh     chan interface{}
}

func (w *servant) setListener(listener net.Listener) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.listener = listener
}

func (w *servant) shutdown() {
	w.registerShutdownCh <- true
	w.workShutdownCh <- true
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.listener.Close()
}

// NewRPCWorker returns a new RPCWorker.
func NewRPCWorker(params Params) wk.Worker {
	checked := checkParams(&params)
	jobRequestCh := make(chan *jobRequest, 0)
	shutdownCh := make(chan interface{}, 0)

	worker := &servant{
		Params:             checked,
		jobRequestCh:       jobRequestCh,
		registerShutdownCh: make(chan interface{}, 1),
		shutdownCh:         shutdownCh,
		workShutdownCh:     make(chan interface{}, 1),
		delegate: &WkDelegate{
			jobRequestCh: jobRequestCh,
			shutdownCh:   shutdownCh,
		},
	}

	go worker.loopPerformJobs()
	go worker.loopRegistration()
	go worker.loopShutdown()
	return worker
}
