package fileaccess

import "github.com/protoman92/mit-distributed-system/src/rpcutil"

// Params represents the required parameters to build a FileAccessor.
type Params struct {
	AccessMethod string
	Network      string
	RPCCaller    rpcutil.Caller
}

// AccessParams represents the required parameters to build an access request.
type AccessParams struct {
	Address string
	File    string
}

// AccessArgs represents the parameters for an access request.
type AccessArgs struct {
	File string
}

// AccessReply represents the reply to an access request.
type AccessReply struct {
	Data []byte
}

// AccessCallResult represents the result of an access invocation.
type AccessCallResult struct {
	ErrCh   chan error
	Request AccessArgs
	Reply   *AccessReply
}
