package mrutil

const (
	// Idle means the task is ready to be handled off.
	Idle TaskStatus = 1

	// InProgress means the task is being performed.
	InProgress TaskStatus = 2

	// Completed means the task is completed.
	Completed TaskStatus = 3

	// Map represents the Map task type.
	Map TaskType = 1

	// Reduce represents the Reduce task type.
	Reduce TaskType = 2

	// UnassignedWorker means that a task has yet to be assigned to any worker.
	UnassignedWorker = "UNASSIGNED"
)

// KeyValue represents a key-value pair emitted by a Map operation.
type KeyValue struct {
	Key   string
	Value string
}

// TaskStatus represents a task's completion status.
type TaskStatus uint

// TaskType represents a task's type (Map or Reduce).
type TaskType uint
