package master

import (
	"sync"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

// Master represents a master that receives job requests from some client and
// distributes them to workers.
type Master interface {
	ErrorChannel() <-chan error
	ShutdownChannel() <-chan interface{}
}

type master struct {
	*Params
	delegate      *MstDelegate
	mutex         *sync.RWMutex
	rpcHandler    rpchandler.Handler
	workers       []string
	shutdownCh    chan interface{}
	errCh         chan error
	workerQueueCh chan string
}

// NewMaster returns a new Master.
func NewMaster(params Params) Master {
	checked := checkParams(&params)
	delegate := newDelegate()
	checkDelegate(delegate)

	master := &master{
		Params:        checked,
		delegate:      delegate,
		mutex:         &sync.RWMutex{},
		rpcHandler:    rpchandler.NewHandler(checked.RPCParams, delegate),
		workers:       make([]string, 0),
		errCh:         make(chan error, 0),
		shutdownCh:    make(chan interface{}, 0),
		workerQueueCh: make(chan string, params.ExpectedWorkerCount),
	}

	checkMaster(master)
	go master.loopError()
	go master.loopJobCompletion()
	go master.loopJobRequest()
	go master.loopRegister()
	go master.loopShutdown()
	go master.loopWorker()
	return master
}
