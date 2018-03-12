package localstate

import (
	"time"
)

// Params represents the required parameters to build a local state.
type Params struct {
	Latency time.Duration // Use this to simulate long-running operations.
}
