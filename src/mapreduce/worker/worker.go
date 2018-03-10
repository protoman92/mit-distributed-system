package worker

import (
	"net"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mapper"
	"github.com/protoman92/mit-distributed-system/src/util"
)

// Worker represents a MapReduce worker. In a distributed system, master(s) will
// hand work to registered workers.
type Worker interface{}

// Params represents the required parameters to build a RPC worker.
type Params struct {
	Address              string
	LogMan               util.LogMan
	Mapper               mapper.Mapper
	MasterAddress        string
	MasterRegisterMethod string
	Network              string
}

func checkParams(params *Params) *Params {
	if params.Address == "" ||
		params.Mapper == nil ||
		params.MasterAddress == "" ||
		params.MasterRegisterMethod == "" ||
		params.Network == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{})
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
func NewRPCWorker(params Params) Worker {
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
