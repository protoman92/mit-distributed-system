package orchestrator

func (o *orchestrator) DoneChannel() <-chan interface{} {
	return o.Executor.DoneChannel()
}

func (o *orchestrator) ErrorChannel() <-chan error {
	return o.errCh
}
