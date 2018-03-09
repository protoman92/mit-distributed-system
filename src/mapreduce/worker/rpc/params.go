package rpc

// RegisterParams represents the required parameters to register a worker.
type RegisterParams struct {
	WorkerAddress string
}

// RegisterReply represents the reply to a registration request.
type RegisterReply struct{}

// JobParams represents the required parameters to perform a job.
type JobParams struct {
	Data []byte
}

// JobReply represents the reply to a job request.
type JobReply struct{}
