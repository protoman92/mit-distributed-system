package worker

// Error represents a worker error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}

func (w *worker) loopError() {
	for {
		select {
		case <-w.shutdownCh:
			return

		case err := <-w.rpcHandler.ErrorChannel():
			w.errCh <- &Error{Original: err}
		}
	}
}

func (w *worker) ErrorChannel() <-chan error {
	return w.errCh
}
