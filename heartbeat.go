package distlimiter

import (
	"time"
)

type Heartbeat struct {
	distlimiter *DistLimiter
	interval time.Duration // second
}

func NewHeartbeat(interval int, distlimiter *DistLimiter) *Heartbeat {
	if interval < 1 {
		interval = 1
	}
	return &Heartbeat{
		distlimiter: distlimiter,
		interval: time.Duration(interval) * time.Second,
	}
}

func (hb *Heartbeat) Go() {
	go func() {
		tick := time.Tick(hb.interval)
		for {
			select {
			case <-tick:
				hb.distlimiter.Pull()
				hb.distlimiter.Send()
			}
		}
	}()
}