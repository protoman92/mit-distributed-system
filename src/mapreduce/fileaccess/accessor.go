package fileaccess

import (
	"github.com/protoman92/mit-distributed-system/src/rpcutil"
)

// FileAccessor accesses files using some URI at some address. This is useful
// for accessing remote files during a Reduce process.
type FileAccessor interface {
	AccessFile(params AccessParams, fn func([]byte) error) error
}

type fileAccessor struct {
	*Params
}

func (a *fileAccessor) AccessFile(params AccessParams, fn func([]byte) error) error {
	args := AccessArgs{File: params.File}
	reply := &AccessReply{Data: make([]byte, 0)}

	callParams := rpcutil.CallParams{
		Args:    args,
		Reply:   reply,
		Method:  a.AccessMethod,
		Network: a.Network,
		Target:  params.Address,
	}

	if err := a.RPCCaller.Call(callParams); err != nil {
		return err
	}

	return fn(reply.Data)
}

// NewFileAccessor returns a new FileAccessor.
func NewFileAccessor(params Params) FileAccessor {
	checked := checkParams(&params)
	accessor := &fileAccessor{Params: checked}
	return accessor
}
