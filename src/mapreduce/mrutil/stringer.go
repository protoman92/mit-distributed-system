package mrutil

func (t TaskType) String() string {
	switch t {
	case Map:
		return "Map"

	case Reduce:
		return "Reduce"

	default:
		panic("Invalid task type")
	}
}
