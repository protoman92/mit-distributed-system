package rpc

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/rpcutil"
	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// Params represents the required parameters to build a RPC worker.
type Params struct {
	Address              string
	MasterAddress        string
	MasterRegisterMethod string
	Network              string
}

// This communicates with the master via RPC.
type servant struct {
	*Params
	delegate *WkDelegate
	mutex    sync.RWMutex
	listener net.Listener
}

func (w *servant) loopRegistration() {
	rpcs := rpc.NewServer()
	rpcs.Register(w.delegate)
	listener, err := net.Listen(w.Network, w.Address)

	if err != nil {
		panic(err)
	}

	w.setListener(listener)
	w.registerWithMaster()
}

func (w *servant) registerWithMaster() {
	registerParams := &RegisterParams{WorkerAddress: w.Params.Address}
	registerReply := &RegisterReply{}

	params := &rpcutil.CallParams{
		Args:    registerParams,
		Method:  w.Params.MasterRegisterMethod,
		Network: w.Params.Network,
		Reply:   registerReply,
		Target:  w.Params.MasterAddress,
	}

	if err := rpcutil.Call(params); err != nil {
		panic(err)
	}
}

func (w *servant) setListener(listener net.Listener) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.listener = listener
}

// NewRPCWorker returns a new RPCWorker.
func NewRPCWorker(params Params) wk.Worker {
	worker := &servant{
		Params:   &params,
		delegate: &WkDelegate{},
	}

	go worker.loopRegistration()
	return worker
}
