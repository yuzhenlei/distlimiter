package distlimiter

import (
	"context"
	"github.com/yuzhenlei/distlimiter/ratelimiter"
	"github.com/yuzhenlei/distlimiter/remotestore"
)

var (
	defaultHeartbeatInterval = 10
)

type RemoteStore interface {
	Send(string) error
	Pull() ([]string, error)
}

type RateLimiter interface {
	Wait(context.Context, bool) error
	SetLimit(uint32)
}

type DistLimiter struct {
	limiter RateLimiter
	heartbeat *Heartbeat
	remote RemoteStore
	peer *Peer
}

func NewDistLimiter(totalQPS uint32, remote RemoteStore, limiter RateLimiter) *DistLimiter {
	if remote == nil {
		remote = remotestore.NewRedis("foobar")
	}
	if limiter == nil {
		limiter = ratelimiter.NewRate(0)
	}
	distlimiter := &DistLimiter{}
	distlimiter.limiter = limiter
	distlimiter.heartbeat = NewHeartbeat(defaultHeartbeatInterval, distlimiter)
	distlimiter.peer = NewPeer(totalQPS, remote)

	distlimiter.heartbeat.Go()

	return distlimiter
}

func (dlimiter *DistLimiter) Wait(ctx context.Context, returnIfUnavailable bool) error {
	return dlimiter.limiter.Wait(ctx, returnIfUnavailable)
}

func (dlimiter *DistLimiter) Send() {
	onSendDone := func(err error) {
		if err != nil {
			dlimiter.limiter.SetLimit(0)
		}
	}
	dlimiter.peer.Send(onSendDone)
}

func (dlimiter *DistLimiter) Pull() {
	onPullDone := func(err error) {
		if err != nil {
			dlimiter.limiter.SetLimit(0)
		} else {
			dlimiter.limiter.SetLimit(dlimiter.peer.GetQPS())
		}
	}
	dlimiter.peer.Pull(onPullDone)
}