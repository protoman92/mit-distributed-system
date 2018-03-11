package rpchandler

func (h *handler) ErrorChannel() <-chan error {
	return h.errCh
}
