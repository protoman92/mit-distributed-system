package worker

// Error represents a worker error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}
