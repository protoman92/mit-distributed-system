package worker

import (
	"time"
)

func (w *worker) shutdown() {
	go func() {
		for {
			select {
			case w.shutdownCh <- true:
				continue

			case <-time.After(time.Second):
				w.LogMan.Printf("%v: finished shutting down all processes.\n", w)
				return
			}
		}
	}()
}

func (w *worker) loopShutdown() {
	for {
		select {
		case <-w.RPCHandler.ShutdownChannel():
			w.LogMan.Printf("%v: received shutdown request.\n", w)
			w.shutdown()
			return
		}
	}
}
