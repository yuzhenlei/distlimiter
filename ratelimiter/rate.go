package ratelimiter

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"sync"
	"time"
)

// https://pkg.go.dev/golang.org/x/time/rate

type RateAdaptor struct {
	cond *sync.Cond
	limit uint32
	limiter *rate.Limiter
}

func NewRate(burst uint32) *RateAdaptor {
	return &RateAdaptor{
		limiter: rate.NewLimiter(0, int(burst)),
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (adaptor *RateAdaptor) Wait(ctx context.Context, returnIfUnavailable bool) error {
	adaptor.cond.L.Lock()
	once := sync.Once{}
	for adaptor.limit == 0 {
		if returnIfUnavailable {
			adaptor.cond.L.Unlock()
			return fmt.Errorf("any requests are not allowed now")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			once.Do(func() {
				if ddl, ok := ctx.Deadline(); ok {
					time.AfterFunc(ddl.Sub(time.Now()), func() {
						adaptor.cond.Broadcast()
					})
				}
			})
		}
		adaptor.cond.Wait()
	}
	defer adaptor.cond.L.Unlock()
	// FIXME 这里如果limit又重新为0，就bug了
	if err := adaptor.limiter.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func (adaptor *RateAdaptor) SetLimit(limit uint32) {
	adaptor.cond.L.Lock()
	adaptor.limit = limit
	adaptor.limiter.SetLimit(rate.Limit(limit))
	adaptor.limiter.SetBurst(int(limit))
	adaptor.cond.L.Unlock()
	if limit > 0 {
		adaptor.cond.Broadcast()
	}
}