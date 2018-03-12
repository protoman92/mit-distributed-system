package worker

import (
	"fmt"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// AcceptJob accepts a job request. If this worker machine does not have the
// file specified by the job request, we should return an error.
func (d *WkDelegate) AcceptJob(request JobRequest, reply *JobReply) error {
	resultCh := make(chan error, 0)
	d.jobCh <- JobCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (w *worker) loopJobReceipt() {
	for {
		select {
		case <-w.shutdownCh:
			return

		case result := <-w.delegate.jobCh:
			w.LogMan.Printf("%v: received job request %v\n", w, result.Request)
			w.jobQueueCh <- result.Request
			result.ErrCh <- nil
		}
	}
}

func (w *worker) loopJobRequest() {
	for {
		select {
		case <-w.shutdownCh:
			return

		case request := <-w.jobQueueCh:
			handleJobRequest := func() error {
				return w.handleJobRequest(request)
			}

			if err := w.RPCParams.RetryWithDelay(handleJobRequest)(); err == nil {
				completeJob := func() error {
					return w.completeJobRequest(request)
				}

				if err1 := w.RPCParams.RetryWithDelay(completeJob)(); err1 != nil {
					w.errCh <- err1
				}
			} else {
				w.errCh <- err
			}

			// Take a look at the code for register loop for the other side of this
			// channel.
			<-w.capacityCh
		}
	}
}

func (w *worker) handleJobRequest(r JobRequest) error {
	switch r.Type {
	case mrutil.Map:
		return w.doMap(r)

	case mrutil.Reduce:
		return w.doReduce(r)

	default:
		panic(fmt.Sprintf("Invalid type %v", r.Type))
	}
}

func (w *worker) completeJobRequest(r JobRequest) error {
	cloned := r.Clone()
	cloned.FilePath = w.FileAccessor.FormatURI(cloned.FilePath)

	request := Task{
		JobRequest: cloned,
		Status:     mrutil.Completed,
		Worker:     w.RPCParams.Address,
	}

	reply := &TaskReply{}

	callParams := rpcutil.CallParams{
		Args:    request,
		Method:  w.MasterCompleteJobMethod,
		Network: w.RPCParams.Network,
		Reply:   reply,
		Target:  w.MasterAddress,
	}

	return w.rpcHandler.Call(callParams)
}
