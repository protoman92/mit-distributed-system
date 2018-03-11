package master

func (m *master) registerWorker(w string) {
	m.mutex.RLock()
	var existing bool

	for ix := range m.workers {
		if m.workers[ix] == w {
			existing = true
			break
		}
	}

	m.mutex.RUnlock()

	if !existing {
		m.LogMan.Printf("%v: adding new worker %s\n", m, w)
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.workers = append(m.workers, w)
	}
}
