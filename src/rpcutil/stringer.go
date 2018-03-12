package rpcutil

import (
	"fmt"
)

func (p *CallParams) String() string {
	return fmt.Sprintf("Calling RPC with args %v, target %s", p.Args, p.Target)
}
