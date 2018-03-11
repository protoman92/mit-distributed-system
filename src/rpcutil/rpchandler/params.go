package rpchandler

import "github.com/protoman92/mit-distributed-system/src/util"

// Params represents the required parameters to build a Handler.
type Params struct {
	Address string
	LogMan  util.LogMan
	Network string
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
