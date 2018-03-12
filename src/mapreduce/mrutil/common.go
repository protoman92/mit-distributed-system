package mrutil

const (
	// Idle means the task is ready to be handled off.
	Idle JobStatus = 1

	// InProgress means the task is being performed.
	InProgress JobStatus = 2

	// Completed means the task is completed.
	Completed JobStatus = 3

	// Map represents the Map task type.
	Map JobType = 1

	// Reduce represents the Reduce task type.
	Reduce JobType = 2

	// UnassignedWorker means that a task has yet to be assigned to any worker.
	UnassignedWorker = "UNASSIGNED"
)

// KeyValue represents a key-value pair emitted by a Map operation.
type KeyValue struct {
	Key   string
	Value string
}

// JobStatus represents a task's completion status.
type JobStatus uint

// JobType represents a task's type (Map or Reduce).
type JobType uint

// MapFunc represents a Map function.
type MapFunc func(string, []byte) []KeyValue

// MapFuncName represents a MapFunc name.
type MapFuncName string

// ReduceFunc represents a Reduce function.
type ReduceFunc func(string, []string) KeyValue

// ReduceFuncName represents a ReduceFunc name.
type ReduceFuncName string
