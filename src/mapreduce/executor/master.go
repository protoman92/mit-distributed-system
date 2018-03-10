package executor

import (
	"net"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/rpcutil"
	"github.com/protoman92/mit-distributed-system/src/util"

	mrutil "github.com/protoman92/mit-distributed-system/src/mapreduce/util"
	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// Params represents the required parameters to build a Master. Consult the net
// package (esp. net.Listen) for available parameters.
type Params struct {
	Address              string
	LogMan               util.LogMan
	Network              string
	WorkerDoJobMethod    string
	WorkerShutdownMethod string
}

func checkParams(params *Params) *Params {
	if params.Address == "" ||
		params.Network == "" ||
		params.WorkerDoJobMethod == "" ||
		params.WorkerShutdownMethod == "" {
		panic("Invalid parameters")
	}

	if params.LogMan == nil {
		params.LogMan = util.NewLogMan(util.LogManParams{})
	}

	return params
}

// This is a master that communicates with workers via RPC.
type executor struct {
	*Params
	mutex                 sync.RWMutex
	delegate              *ExcDelegate
	listener              net.Listener
	workers               []string
	doneCh                chan interface{}
	errCh                 chan error
	inputCh               chan *mrutil.KeyValue
	inputShutdownCh       chan interface{}
	jobQueueCh            chan *wk.JobParams
	registerShutdownCh    chan interface{}
	shutdownCh            chan interface{}
	updateWorkerCh        chan string
	workerCh              chan string
	workDistribShutdownCh chan interface{}
}

func (e *executor) DoneChannel() <-chan interface{} {
	return e.doneCh
}

func (e *executor) ErrorChannel() <-chan error {
	return e.errCh
}

func (e *executor) InputReceiptChannel() chan<- *mrutil.KeyValue {
	return e.inputCh
}

func (e *executor) setListener(listener net.Listener) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.listener = listener
}

// This call blocks, so we will need a timeout channel when we distribute jobs
// so that the master knows when to assign the work to another worker.
func (e *executor) distributeWork(params *wk.JobParams, address string) error {
	jobReply := &wk.JobReply{}

	cParams := &rpcutil.CallParams{
		Args:    params,
		Method:  e.WorkerDoJobMethod,
		Network: e.Network,
		Reply:   jobReply,
		Target:  address,
	}

	return rpcutil.Call(cParams)
}

func (e *executor) shutdown() {
	e.inputShutdownCh <- true
	e.registerShutdownCh <- true
	e.workDistribShutdownCh <- true
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.listener.Close()

	for ix := range e.workers {
		go e.shutdownWorker(e.workers[ix])
	}
}

func (e *executor) shutdownWorker(worker string) {
	args := &wk.ShutdownParams{}
	reply := &wk.ShutdownReply{}

	cParams := &rpcutil.CallParams{
		Args:    args,
		Method:  e.WorkerShutdownMethod,
		Network: e.Network,
		Reply:   reply,
		Target:  worker,
	}

	if err := rpcutil.Call(cParams); err != nil {
		e.errCh <- err
	}
}

// NewRPCMasterExecutor returns a new RPCMaster.
func NewRPCMasterExecutor(params Params) Executor {
	checked := checkParams(&params)
	shutdownCh := make(chan interface{}, 0)
	workerCh := make(chan string, 0)

	master := &executor{
		Params:                checked,
		inputCh:               make(chan *mrutil.KeyValue, 0),
		inputShutdownCh:       make(chan interface{}, 0),
		jobQueueCh:            make(chan *wk.JobParams),
		registerShutdownCh:    make(chan interface{}, 0),
		shutdownCh:            shutdownCh,
		updateWorkerCh:        make(chan string, 0),
		workers:               make([]string, 0),
		workerCh:              workerCh,
		workDistribShutdownCh: make(chan interface{}, 0),
		delegate: &ExcDelegate{
			shutdownCh: shutdownCh,
			workerCh:   workerCh,
		},
	}

	go master.loopInput()
	go master.loopRegistration()
	go master.loopShutdown()
	go master.loopUpdateWorker()
	go master.loopWorkDistribution()
	return master
}
