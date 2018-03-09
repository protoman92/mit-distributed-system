package executor

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"

	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
)

// Params represents the required parameters to build a Master. Consult the net
// package (esp. net.Listen) for available parameters.
type Params struct {
	Address string
	Network string
}

// This is a master that communicates with workers via RPC.
type executor struct {
	*Params
	mutex              sync.RWMutex
	delegate           *ExcDelegate
	listener           net.Listener
	doneCh             chan interface{}
	errCh              chan error
	inputCh            chan []byte
	inputShutdownCh    chan interface{}
	registerShutdownCh chan interface{}
	shutdownCh         chan interface{}
	workerCh           chan string
}

func (e *executor) DoneChannel() <-chan interface{} {
	return e.doneCh
}

func (e *executor) ErrorChannel() <-chan error {
	return e.errCh
}

func (e *executor) InputReceiptChannel() chan<- []byte {
	return e.inputCh
}

func (e *executor) setListener(listener net.Listener) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.listener = listener
}

func (e *executor) loopReceiveInput() {
	for {
		select {
		case input := <-e.inputCh:
			fmt.Println(string(input))

		case <-e.inputShutdownCh:
			return
		}
	}
}

func (e *executor) loopRegistration() {
	rpcs := rpc.NewServer()
	rpcs.Register(e.delegate)
	listener, err := net.Listen(e.Network, e.Address)

	if err != nil {
		e.errCh <- &exc.Error{Original: err}
		return
	}

	e.setListener(listener)

	// After setting up a listener for the master address, we can then start
	// accepting connections on another thread.
	go func() {
		for {
			select {
			case <-e.registerShutdownCh:
				return

			default:
				if conn, err := listener.Accept(); err == nil {
					go func() {
						defer conn.Close()
						rpcs.ServeConn(conn)
					}()
				} else {
					e.errCh <- &exc.Error{Original: err}
					return
				}
			}
		}
	}()
}

func (e *executor) loopShutdown() {
	<-e.shutdownCh
	e.shutdown()
}

func (e *executor) shutdown() {
	e.inputShutdownCh <- true
	e.registerShutdownCh <- true
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.listener.Close()
}

// NewRPCMasterExecutor returns a new RPCMaster.
func NewRPCMasterExecutor(params Params) exc.Executor {
	shutdownCh := make(chan interface{}, 0)
	workerCh := make(chan string, 0)

	master := &executor{
		Params:             &params,
		inputShutdownCh:    make(chan interface{}, 1),
		registerShutdownCh: make(chan interface{}, 1),
		shutdownCh:         shutdownCh,
		workerCh:           workerCh,
		delegate: &ExcDelegate{
			shutdownCh: shutdownCh,
			workerCh:   workerCh,
		},
	}

	go master.loopReceiveInput()
	go master.loopRegistration()
	go master.loopShutdown()
	return master
}
