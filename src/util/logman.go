package util

import (
	"fmt"
)

// LogMan represents a log manager.
type LogMan interface {
	Printf(format string, params ...interface{})
}

// LogManParams represents the required parameters to build a LogMan.
type LogManParams struct {
	Log bool
}

type logman struct {
	*LogManParams
}

func (l *logman) Printf(format string, params ...interface{}) {
	if l.Log {
		fmt.Printf(format, params...)
	}
}

// NewLogMan returns a new LogMan.
func NewLogMan(params LogManParams) LogMan {
	return &logman{LogManParams: &params}
}
