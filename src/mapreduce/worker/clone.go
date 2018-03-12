package worker

// Clone clones a job request.
func (r JobRequest) Clone() JobRequest {
	return JobRequest{
		FilePath:      r.FilePath,
		ID:            r.ID,
		MapFuncName:   r.MapFuncName,
		MapOpCount:    r.MapOpCount,
		ReduceOpCount: r.ReduceOpCount,
		Type:          r.Type,
	}
}

// Clone clones a Task.
func (t Task) Clone() Task {
	return Task{
		JobRequest: t.JobRequest.Clone(),
		Status:     t.Status,
		Worker:     t.Worker,
	}
}
