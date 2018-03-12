package master

import (
	"github.com/protoman92/mit-distributed-system/src/mapreduce/worker"
)

// RegisterWorker registers a worker. This method may be invoke several times
// to notify the master that some worker is ready for more tasks.
func (d *MstDelegate) RegisterWorker(request worker.RegisterRequest, reply *worker.ResigterReply) error {
	resultCh := make(chan error, 0)
	d.registerWorkerCh <- worker.RegisterCallResult{Request: request, ErrCh: resultCh}
	return <-resultCh
}

func (m *master) loopRegister() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case result := <-m.delegate.registerWorkerCh:
			// This is in a goroutine because the worker queue loop waits for idle
			// task one-by-one before accepting new workers.
			go func() {
				m.workerQueueCh <- result.Request.WorkerAddress
			}()

			m.registerWorker(result.Request.WorkerAddress)
			result.ErrCh <- nil
		}
	}
}

func (m *master) registerWorker(w string) {
	m.mutex.RLock()
	var existing bool

	for ix := range m.workers {
		if m.workers[ix] == w {
			existing = true
			break
		}
	}

	m.mutex.RUnlock()

	if !existing {
		m.LogMan.Printf("%v: adding new worker %s\n", m, w)
		go m.loopPing(w)
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.workers = append(m.workers, w)
	}
}
