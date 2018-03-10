package executor

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/protoman92/mit-distributed-system/src/mapreduce/util"

	wk "github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// We create jobs and deposit them into the job queue. The job queue will then
// deliver jobs to workers and retake failed jobs.
func (e *executor) loopInput() {
	inputCh := e.inputCh
	jobNumber := uint(0)
	resetCh := make(chan interface{}, 1)
	var job *wk.JobParams
	var jobCh chan *wk.JobParams

	for {
		select {
		case kv, ok := <-inputCh:
			if !ok {
				e.LogMan.Printf("%v:, Finished receiving inputs", e)
				return
			}

			fmt.Println(kv)

			inputCh = nil
			jobNumber++
			jobCh = e.jobQueueCh

			job = &wk.JobParams{
				Data:      kv.Value,
				Key:       kv.Key,
				JobNumber: jobNumber,
				JobType:   util.Map,
			}

		case jobCh <- job:
			jobCh = nil
			resetCh <- true

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		case <-resetCh:
			inputCh = e.inputCh

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
		e.errCh <- &Error{Original: err}
		return
	}

	e.setListener(listener)

	// After setting up a listener for the master address, we can then start
	// accepting connections on another thread.
	go func() {
		for {
			select {
			case <-e.registerShutdownCh:
				e.LogMan.Printf("Shutting down registration")
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
					e.errCh <- &Error{Original: err}
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

func (e *executor) loopUpdateWorker() {
	for {
		select {
		case worker := <-e.updateWorkerCh:
			e.mutex.Lock()
			var existing bool

			for ix := range e.workers {
				if e.workers[ix] == worker {
					existing = true
					break
				}
			}

			if !existing {
				e.LogMan.Printf("Adding worker %s\n", worker)
				e.workers = append(e.workers, worker)
			}

			e.mutex.Unlock()
		}
	}
}

func (e *executor) loopWorkDistribution() {
	jobCh := e.jobQueueCh
	resetCh := make(chan interface{}, 1)
	var job *wk.JobParams
	var workerCh chan string

	for {
		select {
		case job = <-jobCh:
			jobCh = nil
			workerCh = e.workerCh

		case worker := <-workerCh:
			e.updateWorkerCh <- worker
			jobCopy := job

			// We perform the actual call in a goroutine to avoid blocking the
			// distribution process. If this returns an error, deposit the job back
			// into the queue.
			go func() {
				if err := e.distributeWork(jobCopy, worker); err != nil {
					e.LogMan.Printf("Received job error: %v\n", err)
					e.jobQueueCh <- jobCopy
				}
			}()

			workerCh = nil
			job = nil
			resetCh <- true

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		case <-resetCh:
			jobCh = e.jobQueueCh

		case <-e.workDistribShutdownCh:
			e.LogMan.Printf("Shutting down work distribution")
			return
		}
	}
}
