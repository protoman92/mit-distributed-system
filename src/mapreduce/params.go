package mapreduce

// Params represents all the required parameters to build a MapReduce
// process.
type Params struct {
	NMap          int    // Number of Map jobs
	NReduce       int    // Number of Reduce jobs
	File          string // Name of input file
	InputDir      string
	OutputDir     string
	MasterAddress string
}
