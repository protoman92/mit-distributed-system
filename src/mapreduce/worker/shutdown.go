package worker

func (w *worker) shutdown() {}

func (w *worker) loopShutdown() {
	for {
		select {
		case <-w.rpcHandler.ShutdownChannel():
			w.LogMan.Printf("%v: received shutdown request.\n", w)
			w.shutdown()
			return
		}
	}
}
