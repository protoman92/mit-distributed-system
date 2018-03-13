package worker

import (
	"fmt"
	"os"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/job"
	"github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// AcceptJob accepts a job request. If this worker machine does not have the
// file specified by the job request, we should return an error.
func (d *WkDelegate) AcceptJob(request job.WorkerJob, reply *JobReply) error {
	job.CheckWorkerJob(request)
	resultCh := make(chan error, 0)
	d.jobCh <- JobCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (w *worker) loopJobReceipt() {
	for {
		select {
		case <-w.shutdownCh:
			return

		case result := <-w.Delegate.jobCh:
			w.LogMan.Printf("%v: received job request %v\n", w, result.Request)

			// For a Map operation, the file must be available locally.
			if result.Request.Type == mrutil.Map {
				if _, err := os.Stat(result.Request.File); err != nil {
					result.ErrCh <- err
					break
				}
			}

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
			go func() {
				handleJobRequest := func() error { return w.handleJobRequest(request) }

				if err := w.RPCParams.RetryWithDelay(handleJobRequest)(); err != nil {
					w.errCh <- err
				} else {
					completeJob := func() error { return w.completeJobRequest(request) }

					if err1 := w.RPCParams.RetryWithDelay(completeJob)(); err1 != nil {
						w.errCh <- err1
					} else {
						w.LogMan.Printf("%v: completed request %v.\n", w, request)
					}
				}

				// Take a look at the code for register loop for the other side of this
				// channel.
				<-w.capacityCh
			}()
		}
	}
}

func (w *worker) handleJobRequest(r job.WorkerJob) error {
	switch r.Type {
	case mrutil.Map:
		return w.Mapper.DoMap(r)

	case mrutil.Reduce:
		return w.Reducer.DoReduce(r)
	}

	panic(fmt.Sprintf("Invalid type %v", r.Type))
}

func (w *worker) completeJobRequest(r job.WorkerJob) error {
	cloned := r.Clone()
	cloned.RemoteFileAddr = w.RPCParams.Address
	reply := &JobReply{}

	callParams := rpcutil.CallParams{
		Args:    cloned,
		Method:  w.MasterCompleteJobMethod,
		Network: w.RPCParams.Network,
		Reply:   reply,
		Target:  w.MasterAddress,
	}

	return w.RPCHandler.Call(callParams)
}
