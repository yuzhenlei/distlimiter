package ratelimiter

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"sync"
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
	for adaptor.limit == 0 {
		if returnIfUnavailable {
			adaptor.cond.L.Unlock()
			return fmt.Errorf("any requests are not allowed now")
		}
		adaptor.cond.Wait()
	}
	defer adaptor.cond.L.Unlock()
	if err := adaptor.limiter.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func (adaptor *RateAdaptor) SetLimit(limit uint32) {
	adaptor.cond.L.Lock()
	adaptor.limit = limit
	adaptor.cond.L.Unlock()
	adaptor.limiter.SetLimit(rate.Limit(limit))
	adaptor.limiter.SetBurst(int(limit))
	if limit > 0 {
		adaptor.cond.Broadcast()
	}
}