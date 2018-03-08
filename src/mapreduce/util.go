package mapreduce

import "strconv"

// MapName returns the name of the file that is the input for map job <MapJob>
func MapName(name string, MapJob int) string {
	return "mrtmp." + name + "-" + strconv.Itoa(MapJob)
}

// ReduceName returns a name of the Reduce job result.
func ReduceName(fileName string, MapJob int, ReduceJob int) string {
	return MapName(fileName, MapJob) + "-" + strconv.Itoa(ReduceJob)
}

// MergeName merges file name.
func MergeName(fileName string, ReduceJob int) string {
	return "mrtmp." + fileName + "-res-" + strconv.Itoa(ReduceJob)
}
