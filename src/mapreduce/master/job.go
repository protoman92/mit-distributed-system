package master

func (m *master) loopJobRequest() {
	for {
		select {
		case result := <-m.jobRequestCh:
			m.LogMan.Printf("%v: received job %v\n", m, result.request)
			result.errCh <- nil
		}
	}
}
