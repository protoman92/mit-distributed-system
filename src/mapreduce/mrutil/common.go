package mrutil

const (
	// Idle means the task is ready to be handled off.
	Idle Status = 0

	// InProgress means the task is being performed.
	InProgress Status = 1

	// Completed means the task is completed.
	Completed Status = 2

	// Map represents the Map task type.
	Map Type = 0

	// Reduce represents the Reduce task type.
	Reduce Type = 1
)

// Status represents a task's completion status.
type Status uint

// Type represents a task's type (Map or Reduce).
type Type uint
