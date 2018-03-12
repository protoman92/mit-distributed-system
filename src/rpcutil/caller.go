package rpcutil

// Caller is an object that calls RPC methods.
type Caller interface {
	Call(params CallParams) error
}

type caller struct{}

func (c *caller) Call(params CallParams) error {
	return Call(params)
}

// NewCaller returns a new Caller.
func NewCaller() Caller {
	return &caller{}
}
