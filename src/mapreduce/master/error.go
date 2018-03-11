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
		case err := <-m.rpcHandler.ErrorChannel():
			m.errCh <- &Error{Original: err}
		}
	}
}

func (m *master) ErrorChannel() <-chan error {
	return m.errCh
}
