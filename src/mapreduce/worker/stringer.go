package worker

import (
	"fmt"
)

func (w *worker) String() string {
	return fmt.Sprintf("Worker %s", w.RPCParams.Address)
}

func (r JobRequest) String() string {
	return fmt.Sprintf("Job request %v: %s", r.Type, r.FilePath)
}

func (t *Task) String() string {
	return fmt.Sprintf(
		"Task for request %v, status %v, assigned to %s",
		t.JobRequest,
		t.Status,
		t.Worker,
	)
}
