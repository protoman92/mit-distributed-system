package worker

// WkDelegate represents a RPC Delegate for a worker.
type WkDelegate struct{}

func newDelegate() *WkDelegate {
	return &WkDelegate{}
}
