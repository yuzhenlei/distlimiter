package distlimiter

import (
	"context"
	"github.com/yuzhenlei/distlimiter/ratelimiter"
	"github.com/yuzhenlei/distlimiter/remotestore"
	"time"
)

const (
	defaultHeartbeatSeconds = 30
)

var (
	defaultRemoteStore = func(options *remotestore.RedisOptions) RemoteStore {
		return remotestore.NewRedis(options)
	}
	defaultLimiter = func(options *ratelimiter.RateOptions) RateLimiter {
		return ratelimiter.NewRate(options)
	}
)

type RemoteStore interface {
	Send(now time.Time, entry string) error
	Pull(min time.Time, max time.Time) ([]string, error)
}

type RateLimiter interface {
	Wait(context.Context, bool) error
	SetLimit(uint32)
}

type DistLimiter struct {
	limiter RateLimiter
	remote RemoteStore
	peer *Peer
}

type Options struct {
	Id string
	TotalQPS uint32
	HeartbeatSeconds uint32
	Limiter RateLimiter
	Remote RemoteStore
	RedisAddr string
	RedisKey string
}

func NewDistLimiter(options *Options) *DistLimiter {
	distlimiter := &DistLimiter{}

	limiter := options.Limiter
	if options.Limiter == nil {
		limiter = defaultLimiter(nil)
	}
	distlimiter.limiter = limiter

	remote := options.Remote
	if remote == nil {
		remote = defaultRemoteStore(&remotestore.RedisOptions{
			Addr: options.RedisAddr,
			Key:  options.RedisKey,
		})
	}
	peerOptions := &peerOptions{
		Id:               options.Id,
		HeartbeatSeconds: options.HeartbeatSeconds,
		OnSendDone:       distlimiter.onSendDone,
		OnPullDone:       distlimiter.onPullDone,
	}
	distlimiter.peer = NewPeer(options.TotalQPS, remote, peerOptions)

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