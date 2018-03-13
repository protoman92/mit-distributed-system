package master

import (
	"time"
)

func (m *master) ShutdownChannel() <-chan interface{} {
	return m.shutdownCh
}

func (m *master) shutdown() {
	// Repeatedly send shutdown events until all loops have been cleaned up.
	go func() {
		for {
			select {
			case m.shutdownCh <- true:
				continue

			case <-time.After(time.Duration(time.Second)):
				m.LogMan.Printf("%v: finished shutting down all processes.\n", m)
				return
			}
		}
	}()

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for ix := range m.workers {
		go func(worker string) {
			m.shutdownWorker(worker)
		}(m.workers[ix])
	}
}

func (m *master) shutdownWorker(workerAddress string) {
	if err := m.RPCHandler.Shutdown(m.RPCParams.Network, workerAddress); err != nil {
		go func() {
			m.errCh <- err
		}()
	}
}

func (m *master) loopShutdown() {
	for {
		select {
		case <-m.RPCHandler.ShutdownChannel():
			m.LogMan.Printf("%v: received shutdown request.\n", m)
			m.shutdown()
			return
		}
	}
}
