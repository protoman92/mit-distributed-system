package master

import (
	"net"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/util"
)

// Master represents a master that receives job requests from some client and
// distributes them to workers.
type Master interface{}

// Params represents the required parameters to build a Master.
type Params struct {
	Address string
	LogMan  util.LogMan
	Network string
}

type master struct {
	*Params
	delegate      *MstDelegate
	mutex         *sync.RWMutex
	listener      net.Listener
	errCh         chan error
	jobRequestCh  chan *JobCallResult
	rpcShutdownCh chan interface{}
	shutdownCh    chan *ShutdownCallResult
}

// NewMaster returns a new Master.
func NewMaster(params Params) Master {
	checked := checkParams(&params)
	jobRequestCh := make(chan *JobCallResult, 0)
	shutdownCh := make(chan *ShutdownCallResult, 0)

	delegate := &MstDelegate{
		jobRequestCh: jobRequestCh,
		shutdownCh:   shutdownCh,
	}

	checkDelegate(delegate)

	master := &master{
		Params:        checked,
		delegate:      delegate,
		mutex:         &sync.RWMutex{},
		errCh:         make(chan error, 0),
		jobRequestCh:  jobRequestCh,
		rpcShutdownCh: make(chan interface{}, 0),
		shutdownCh:    shutdownCh,
	}

	checkMaster(master)
	master.startRPCServer()
	go master.loopError()
	go master.loopJobRequest()
	go master.loopShutdownRequest()
	return master
}
