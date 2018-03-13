package master

// Error represents a Master error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}

func (m *master) loopError() {
	for {
		select {
		case <-m.shutdownCh:
			return

		case err := <-m.RPCHandler.ErrorChannel():
			m.errCh <- &Error{Original: err}
		}
	}
}

func (m *master) ErrorChannel() <-chan error {
	return m.errCh
}
