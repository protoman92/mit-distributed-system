package rpc

// RegisterParams represents the required parameters to register a worker.
type RegisterParams struct {
	WorkerAddress string
}

// RegisterReply represents the reply to a registration request.
type RegisterReply struct{}
