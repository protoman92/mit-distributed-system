package rpc

import (
	"fmt"
)

// WkDelegate represents a RPC Worker's exported delegate that transmits events
// back to said worker.
type WkDelegate struct{}

// DoWork accepts a job via RPC, performs it and sends back a reply.
func (d *WkDelegate) DoWork(args *JobParams, reply *JobReply) error {
	fmt.Printf("received job %v\n", len(args.Data))
	return nil
}
