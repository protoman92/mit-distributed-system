package master

import (
	"net"
	"net/rpc"
)

// Start the master's RPC server and loop connection acceptance.
func (m *master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m.delegate)
	listener, err := net.Listen(m.Network, m.Address)

	if err != nil {
		m.errCh <- &Error{Original: err}
		return
	}

	m.mutex.Lock()
	m.listener = listener
	m.mutex.Unlock()

	// After setting up a listener for the master address, we can then start
	// accepting connections on another thread.
	go func() {
		for {
			select {
			case <-m.rpcShutdownCh:
				m.LogMan.Printf("%v: shutting down RPC server", m)
				return

			default:
				// When a RPC call is received, we serve the delegate's methods. The
				// delegate then relays back the necessary information to the master.
				if conn, err := listener.Accept(); err == nil {
					go func() {
						defer conn.Close()
						rpcs.ServeConn(conn)
					}()
				} else {
					m.errCh <- &Error{Original: err}
					return
				}
			}
		}
	}()
}
