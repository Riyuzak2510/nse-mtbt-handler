package broker

func (a *AsyncPublisher) Connected() bool { return a.client.Conn.IsConnected() }
