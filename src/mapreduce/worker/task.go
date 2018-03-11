package worker

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

// Task represents a Map/Reduce task.
type Task struct {
	*JobRequest
	Status mrutil.TaskStatus
	Worker string
}

// IsIdle checks if a task is idle.
func (t *Task) IsIdle() bool {
	return t.Status == mrutil.Idle
}

// IsInProgress checks if a task is in-progress.
func (t *Task) IsInProgress() bool {
	return t.Status == mrutil.InProgress
}

// IsCompleted checks if a task is completed.
func (t *Task) IsCompleted() bool {
	return t.Status == mrutil.Completed
}
