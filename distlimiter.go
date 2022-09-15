package distlimiter

import (
	"context"
	"github.com/yuzhenlei/distlimiter/ratelimiter"
	"github.com/yuzhenlei/distlimiter/remotestore"
	"time"
)

var (
	defaultHeartbeatInterval = 10
)

type RemoteStore interface {
	Send(now time.Time, entry string) error
	Pull(min time.Time, max time.Time) ([]string, error)
}

type RateLimiter interface {
	Wait(context.Context, bool) error
	SetLimit(uint32)
}

// TODO WithOption

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
	distlimiter.peer = NewPeer(totalQPS, remote)
	distlimiter.peer.onSendDone = func(err error) {
		distlimiter.onSendDone(err)
	}
	distlimiter.peer.onPullDone = func(err error) {
		distlimiter.onPullDone(err)
	}
	return distlimiter
}

func (dlimiter *DistLimiter) Wait(ctx context.Context, returnIfUnavailable bool) error {
	return dlimiter.limiter.Wait(ctx, returnIfUnavailable)
}

func (dlimiter *DistLimiter) onSendDone(err error) {
	if err != nil {
		dlimiter.limiter.SetLimit(0)
	}
}

func (dlimiter *DistLimiter) onPullDone(err error) {
	if err != nil {
		dlimiter.limiter.SetLimit(0)
	} else {
		dlimiter.limiter.SetLimit(dlimiter.peer.GetQPS())
	}
}

// TODO 需要一个Close方法吗