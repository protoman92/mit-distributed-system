package worker

import (
	"net"
	"net/rpc"
)

func (w *servant) loopPerformJobs() {
	jobRequestCh := w.jobRequestCh

	for {
		select {
		case request := <-jobRequestCh:
			err := w.Mapper.Map(request.DataChunk())
			request.errCh <- err

			// Register again to notify master that this worker is ready for more
			// work.
			w.registerWithMaster()

		case <-w.workShutdownCh:
			return
		}
	}
}

func (w *servant) loopRegistration() {
	rpcs := rpc.NewServer()
	rpcs.Register(w.delegate)
	listener, err := net.Listen(w.Network, w.Address)

	if err != nil {
		panic(err)
	}

	w.setListener(listener)
	w.registerWithMaster()

	go func() {
		for {
			select {
			case <-w.registerShutdownCh:
				return

			default:
				// Here we expose the worker delegate's methods via RPC so that the
				// master can hand over jobs for said worker to perform.
				if conn, err := listener.Accept(); err == nil {
					defer conn.Close()
					rpcs.ServeConn(conn)
				} else {
					panic(err)
				}
			}
		}
	}()
}

func (w *servant) loopShutdown() {
	<-w.shutdownCh
	w.shutdown()
}
