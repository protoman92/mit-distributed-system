package executor

// Error represents an Executor error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}
