package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// RegisterWorker registers a worker.
func (d *MstDelegate) RegisterWorker(request *worker.RegisterRequest, reply *worker.ResigterReply) error {
	resultCh := make(chan error, 0)
	d.registerWorkerCh <- &worker.RegisterCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) loopRegister() {
	for {
		select {
		case result := <-m.delegate.registerWorkerCh:
			m.registerWorker(result.Request.WorkerAddress)
			result.ErrCh <- nil
		}
	}
}
