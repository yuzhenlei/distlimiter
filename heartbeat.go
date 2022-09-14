package distlimiter

import (
	"time"
)

type Heartbeat struct {
	peer *Peer
	interval time.Duration // second
}

func NewHeartbeat(interval time.Duration, peer *Peer) *Heartbeat {
	if interval < 1 {
		interval = 1
	}
	return &Heartbeat{
		peer: peer,
		interval: interval,
	}
}

func (hb *Heartbeat) Go() {
	go func() {
		tick := time.Tick(hb.interval)
		for {
			select {
			case <-tick:
				hb.peer.Pull()
				hb.peer.Send()
			}
		}
	}()
}