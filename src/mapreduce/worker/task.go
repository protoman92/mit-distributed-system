package worker

import "github.com/protoman92/mit-distributed-system/src/mapreduce/mrutil"

// Task represents a Map/Reduce task.
type Task struct {
	*JobRequest
	Status mrutil.TaskStatus
	Worker string
}
