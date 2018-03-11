package rpchandler

// RPCDelegate represents a RPC Handler delegate.
type RPCDelegate struct {
	shutdownCh chan *ShutdownCallResult
}

func newDelegate() *RPCDelegate {
	return &RPCDelegate{shutdownCh: make(chan *ShutdownCallResult, 0)}
}
