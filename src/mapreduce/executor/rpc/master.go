package executor

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/rpcutil"
	"github.com/protoman92/mit-distributed-system/src/util"

	exc "github.com/protoman92/mit-distributed-system/src/mapreduce/executor"
	erpc "github.com/protoman92/mit-distributed-system/src/mapreduce/worker/rpc"
)

// Params represents the required parameters to build a Master. Consult the net
// package (esp. net.Listen) for available parameters.
type Params struct {
	Address           string
	LogMan            util.LogMan
	Network           string
	WorkerDoJobMethod string
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
	jobErrCh           chan error
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
				// When a RPC call is received, we serve the delegate's methods. The
				// delegate then relays back the necessary information to the master.
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

func (e *executor) loopInputAndWorker() {
	inputCh := e.inputCh
	resetCh := make(chan interface{}, 1)
	var input []byte
	var workerCh chan string

	for {
		select {
		case input = <-inputCh:
			inputCh = nil
			workerCh = e.workerCh

		case worker := <-workerCh:
			workerCh = nil
			params := &DistributeParams{Data: input, WorkerAddress: worker}

			if err := e.distributeWork(params); err != nil {
				e.LogMan.Printf("Received job error: %v\n", err)
			}

			resetCh <- true

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		case <-resetCh:
			inputCh = e.inputCh

		case <-e.inputShutdownCh:
			return
		}
	}
}

// This call blocks, so we will need a timeout channel when we distribute jobs
// so that the master knows when to assign the work to another worker.
func (e *executor) distributeWork(params *DistributeParams) error {
	jobParams := &erpc.JobParams{Data: params.Data}
	jobReply := &erpc.JobReply{}

	cParams := &rpcutil.CallParams{
		Args:    jobParams,
		Method:  e.WorkerDoJobMethod,
		Network: e.Network,
		Reply:   jobReply,
		Target:  params.WorkerAddress,
	}

	return rpcutil.Call(cParams)
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
		inputCh:            make(chan []byte, 0),
		inputShutdownCh:    make(chan interface{}, 0),
		registerShutdownCh: make(chan interface{}, 0),
		shutdownCh:         shutdownCh,
		workerCh:           workerCh,
		delegate: &ExcDelegate{
			shutdownCh: shutdownCh,
			workerCh:   workerCh,
		},
	}

	go master.loopRegistration()
	go master.loopShutdown()
	go master.loopInputAndWorker()
	return master
}
