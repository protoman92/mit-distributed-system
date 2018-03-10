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
		case err := <-m.errCh:
			m.LogMan.Printf("%v: received error %v\n", m, err)
		}
	}
}
