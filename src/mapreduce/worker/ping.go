package worker

// Ping pings a worker for activity.
func (d *WkDelegate) Ping(request PingRequest, reply *PingReply) error {
	reply.OK = true
	return nil
}
