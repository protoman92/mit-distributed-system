package master

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil/rpchandler"
)

func (m *master) shutdown() {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for ix := range m.workers {
		go func(worker string) {
			m.shutdownWorker(worker)
		}(m.workers[ix])
	}
}

func (m *master) shutdownWorker(workerAddress string) {
	m.LogMan.Printf("%v: shutting down worker %s\n", m, workerAddress)

	if err := rpchandler.Shutdown(m.RPCParams.Network, workerAddress); err != nil {
		go func() {
			m.errCh <- err
		}()
	}
}

func (m *master) loopShutdown() {
	for {
		select {
		case <-m.rpcHandler.ShutdownChannel():
			m.LogMan.Printf("%v: received shutdown request.\n", m)
			m.shutdown()
			return
		}
	}
}
