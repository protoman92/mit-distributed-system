package rpchandler

import (
	"fmt"
)

func (h *handler) String() string {
	return fmt.Sprintf("RPC handler for %s", h.Address)
}
