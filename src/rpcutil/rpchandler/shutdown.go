package rpchandler

// Shutdown accepts a shutdown request.
func (d *RPCDelegate) Shutdown(request *ShutdownRequest, reply *ShutdownReply) error {
	resultCh := make(chan error, 0)
	d.shutdownCh <- &ShutdownCallResult{request: request, errCh: resultCh}
	return <-resultCh
}

func (h *handler) ShutdownChannel() <-chan interface{} {
	return h.shutdownCh
}

func (h *handler) shutdown() {
	h.shutdownCh <- true
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.listener.Close()
}

func (h *handler) loopShutdown() {
	for {
		select {
		case result := <-h.delegate.shutdownCh:
			h.LogMan.Printf("%v: shutting down RPC server for %s.\n", h, h.Address)
			h.shutdown()
			result.errCh <- nil
			return
		}
	}
}
