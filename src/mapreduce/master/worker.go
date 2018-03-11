package master

func (m *master) loopWorker() {
	for {
		select {
		case worker := <-m.workerQueueCh:
			m.LogMan.Printf("%v: worker %s is ready to work.\n", m, worker)
		}
	}
}
