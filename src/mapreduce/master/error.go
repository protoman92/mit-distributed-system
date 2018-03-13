package master

// Error represents a Master error.
type Error struct {
	Original error
}

func (e *Error) Error() string {
	return e.Original.Error()
}
