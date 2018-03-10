package master

func (m *master) shutdown() {
	m.rpcShutdownCh <- true
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.listener.Close()
}

func (m *master) loopShutdownRequest() {
	for {
		select {
		case result := <-m.shutdownCh:
			m.LogMan.Printf("%v: received shutdown request\n", m)
			m.shutdown()
			result.errCh <- nil
			return
		}
	}
}
