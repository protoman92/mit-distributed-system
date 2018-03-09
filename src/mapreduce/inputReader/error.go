package inputReader

// Error represents a InputReader error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}
