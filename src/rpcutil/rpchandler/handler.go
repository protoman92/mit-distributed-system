package rpchandler

import (
	"net"
	"sync"
)

// Handler represents a RPC handler.
type Handler interface {
	ErrorChannel() <-chan error
	ShutdownChannel() <-chan interface{}
}

type handler struct {
	*Params
	delegate   *RPCDelegate
	mutex      *sync.RWMutex
	listener   net.Listener
	errCh      chan error
	shutdownCh chan interface{}
}

// NewHandler returns a new Handler.
func NewHandler(params Params, delegate interface{}) Handler {
	checked := checkParams(&params)
	rpcDelegate := newDelegate()
	checkDelegate(rpcDelegate)

	handler := &handler{
		Params:     checked,
		delegate:   rpcDelegate,
		mutex:      &sync.RWMutex{},
		errCh:      make(chan error, 0),
		shutdownCh: make(chan interface{}, 0),
	}

	checkHandler(handler)
	handler.startRPCServer(delegate)
	go handler.loopShutdown()
	return handler
}
