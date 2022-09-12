package ratelimiter

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
)

// https://pkg.go.dev/golang.org/x/time/rate

type RateAdaptor struct {
	limit uint32
	limiter *rate.Limiter
}

func NewRate(limit uint32) *RateAdaptor {
	return &RateAdaptor{
		limiter: rate.NewLimiter(0, int(limit)),
	}
}

func (adaptor *RateAdaptor) Wait(ctx context.Context, returnIfUnavailable bool) error {
	if adaptor.limit == 0 && returnIfUnavailable {
		return fmt.Errorf("any requests are not allowed now")
	}
	if err := adaptor.limiter.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func (adaptor *RateAdaptor) SetLimit(limit uint32) {
	adaptor.limit = limit
	adaptor.limiter.SetLimit(rate.Limit(limit))
	adaptor.limiter.SetBurst(int(limit))
}