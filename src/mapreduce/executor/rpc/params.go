package executor

// DistributeParams represents the necessary parameters to distribute some
// work to a worker.
type DistributeParams struct {
	Data          []byte
	WorkerAddress string
}

// ShutdownParams represents the necessary parameters for a shutdown request.
type ShutdownParams struct{}

// ShutdownReply represents the reply to a shutdown request.
type ShutdownReply struct {
	NJobs int
	OK    bool
}
