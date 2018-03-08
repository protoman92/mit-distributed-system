package readWriter

// ReadWriteError represents a ReadWriter error.
type ReadWriteError struct {
	original error
}

func (e *ReadWriteError) String() string {
	return e.original.Error()
}
