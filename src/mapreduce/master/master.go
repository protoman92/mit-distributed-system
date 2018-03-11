package master

import (
	"sync"

	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

// Master represents a master that receives job requests from some client and
// distributes them to workers.
type Master interface {
	ErrorChannel() <-chan error
}

type master struct {
	*Params
	delegate   *MstDelegate
	mutex      *sync.RWMutex
	rpcHandler rpchandler.Handler
	workers    []string
	errCh      chan error
}

// NewMaster returns a new Master.
func NewMaster(params Params) Master {
	checked := checkParams(&params)
	delegate := newDelegate()
	checkDelegate(delegate)

	master := &master{
		Params:     checked,
		delegate:   delegate,
		mutex:      &sync.RWMutex{},
		rpcHandler: rpchandler.NewHandler(checked.RPCParams, delegate),
		errCh:      make(chan error, 0),
		workers:    make([]string, 0),
	}

	checkMaster(master)
	go master.loopError()
	go master.loopJobRequest()
	go master.loopRegister()
	go master.loopShutdown()
	return master
}
