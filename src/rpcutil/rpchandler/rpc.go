package rpchandler

import (
	"net"
	"net/rpc"

	compose "github.com/protoman92/gocompose/pkg"
)

func (h *handler) startRPCServer(delegate interface{}) {
	startServer := func() error {
		rpcs := rpc.NewServer()
		rpcs.Register(delegate)
		rpcs.Register(h.delegate)
		listener, err := net.Listen(h.Network, h.Address)

		if err != nil {
			return err
		}

		h.mutex.Lock()
		h.listener = listener
		h.mutex.Unlock()

		// After setting up a listener for the master address, we can then start
		// accepting connections on another thread.
		go func() {
			for {
				select {
				default:
					// When a RPC call is received, we serve the delegate's methods. The
					// delegate then relays back the necessary information to the master.
					if conn, err := listener.Accept(); err == nil {
						go func() {
							defer conn.Close()
							rpcs.ServeConn(conn)
						}()
					} else {
						h.errCh <- err
						return
					}
				}
			}
		}()

		return nil
	}

	if err := compose.Retry(startServer, h.RetryCount)(); err != nil {
		go func() {
			h.errCh <- err
		}()
	}
}
