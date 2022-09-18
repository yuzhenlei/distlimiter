package distlimiter

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOneCallerLongRunning(t *testing.T) {
	var (
		redisKey = "TestOneCallerLongRunning"
		testDurationSeconds = 3
		totalQps = 2
		hbSeconds = 2
	)

	options := &Options{
		Id:               "foobar",
		TotalQPS:         uint32(totalQps),
		HeartbeatSeconds: uint32(hbSeconds),
		Limiter:          nil,
		Remote:           nil,
		RedisAddr:        "127.0.0.1:6379",
		RedisKey:         redisKey,
	}

	distlimiter := NewDistLimiter(options)
	time.Sleep(time.Second * time.Duration(hbSeconds + 2))

	numOk := 0

	ctx := context.Background()
	endTime := time.Now().Add(time.Duration(testDurationSeconds) * time.Second)
	for time.Now().Before(endTime) {
		err := distlimiter.Wait(ctx, false)
		if err != nil {
			t.Fatal(err.Error())
		}
		numOk++
	}
	if want := testDurationSeconds * totalQps; numOk > want + totalQps {
		t.Errorf("want[%d] numOk[%d]", want, numOk)
	}
	if want := testDurationSeconds * totalQps; numOk < want - totalQps {
		t.Errorf("want[%d] numOk[%d]", want, numOk)
	}
}

func TestOneCallerWithTimeout(t *testing.T) {
	var (
		redisKey = "TestOneCallerWithTimeout"
		testDurationSeconds = 3
		totalQps = 2
		hbSeconds = 2
	)

	options := &Options{
		Id:               "foobar",
		TotalQPS:         uint32(totalQps),
		HeartbeatSeconds: uint32(hbSeconds),
		Limiter:          nil,
		Remote:           nil,
		RedisAddr:        "127.0.0.1:6379",
		RedisKey:         redisKey,
	}

	distlimiter := NewDistLimiter(options)
	time.Sleep(time.Second * time.Duration(hbSeconds + 2))

	numOk := 0

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(testDurationSeconds) * time.Second)
	for {
		select {
		case <-ctx.Done():
			goto checked
		default:
			err := distlimiter.Wait(ctx, false)
			if err != nil {
				if strings.Contains(err.Error(), "exceed context deadline") {
					goto checked
				} else {
					t.Error(err.Error())
				}
			}
			numOk++
		}
	}
	checked:
	if want := testDurationSeconds * totalQps; numOk > want + totalQps {
		t.Errorf("want[%d] numOk[%d]", want, numOk)
	}
	if want := testDurationSeconds * totalQps; numOk < want - totalQps {
		t.Errorf("want[%d] numOk[%d]", want, numOk)
	}
}

func TestPeerQPSAdjustment(t *testing.T) {
	var (
		redisKey = "TestPeerQPSAdjustment"
		totalQps = 10
		hbSeconds = 1
	)
	callers := []struct {
		id string
		distlimiter *DistLimiter
		wantQPS uint32
	}{
		{"caller0", nil, 4},
		{"caller1", nil, 3},
		{"caller2", nil, 3},
	}
	for key, callerOption := range callers {
		options := &Options{
			Id:               callerOption.id,
			TotalQPS:         uint32(totalQps),
			HeartbeatSeconds: uint32(hbSeconds),
			Limiter:          nil,
			Remote:           nil,
			RedisAddr:        "127.0.0.1:6379",
			RedisKey:         redisKey,
		}
		callers[key].distlimiter = NewDistLimiter(options)
	}
	time.Sleep(time.Second * time.Duration(hbSeconds + 2))

	for key := range callers {
		peerQPS := callers[key].distlimiter.peer.GetQPS()
		if peerQPS != callers[key].wantQPS {
			t.Errorf("wantqps[%d] peer.qps[%d]", callers[key].wantQPS, peerQPS)
		}
	}
}