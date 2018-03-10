package master

// JobRequest represents job request sent to a master.
type JobRequest struct {
	FilePaths []string
}

// JobReply represents the reply to a job request RPC invocation.
type JobReply struct{}

// JobCallResult represents the result of a job request transmission.
type JobCallResult struct {
	request *JobRequest
	errCh   chan error
}

// ShutdownRequest represents the required parameters for a shutdown request.
type ShutdownRequest struct{}

// ShutdownReply represents the reply to a shutdown request.
type ShutdownReply struct{}

// ShutdownCallResult represents the result of a shutdown request transmission.
type ShutdownCallResult struct {
	request *ShutdownRequest
	errCh   chan error
}
