package limiter

import (
	"context"
	"errors"
	"time"

	"github.com/JackBerck/fluxguard/pkg/storage"
)

// LeakyBucketLimiter limits requests using the leaky bucket algorithm.
// Incoming requests are queued and released at a constant rate. When the
// queue is at capacity the request is rejected immediately; otherwise the
// calling goroutine sleeps until its turn arrives. If the caller's context
// is cancelled while waiting the goroutine is unblocked and the request is
// rejected without leaking resources.
//
// LeakyBucketLimiter is safe for concurrent use by multiple goroutines.
type LeakyBucketLimiter struct {
	store    storage.Storage
	capacity float64
	rate     float64
	log      Logger
}

// LeakyBucketOption configures a [LeakyBucketLimiter].
type LeakyBucketOption func(*LeakyBucketLimiter)

// WithLeakyBucketLogger sets the logger used by the limiter.
// Pass nil to disable logging (the default).
func WithLeakyBucketLogger(l Logger) LeakyBucketOption {
	return func(lb *LeakyBucketLimiter) { lb.log = resolveLogger(l) }
}

// NewLeakyBucket returns a LeakyBucketLimiter backed by store.
//
//   - capacity is the maximum number of requests that may wait in the queue; must be > 0.
//   - rate is the number of requests emitted per second; must be > 0.
func NewLeakyBucket(store storage.Storage, capacity, rate float64, opts ...LeakyBucketOption) (*LeakyBucketLimiter, error) {
	if store == nil {
		return nil, errors.New("fluxguard: store must not be nil")
	}
	if capacity <= 0 {
		return nil, errors.New("fluxguard: capacity must be greater than zero")
	}
	if rate <= 0 {
		return nil, errors.New("fluxguard: rate must be greater than zero")
	}

	lb := &LeakyBucketLimiter{
		store:    store,
		capacity: capacity,
		rate:     rate,
		log:      nopLogger{},
	}
	for _, opt := range opts {
		opt(lb)
	}
	return lb, nil
}

// Allow reports whether the request from clientID is permitted.
// When the store assigns a non-zero wait time, Allow blocks until the wait
// elapses or ctx is cancelled. It uses the "leaky:" key prefix internally to
// avoid conflicts with other limiter types sharing the same store.
func (lb *LeakyBucketLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	waitTime, err := lb.store.AllowLeakyBucket(ctx, "leaky:"+clientID, lb.capacity, lb.rate)
	if err != nil {
		lb.log.Error("leaky bucket store error", "clientID", clientID, "err", err)
		return false, err
	}

	if waitTime < 0 {
		lb.log.Debug("leaky bucket denied: queue full", "clientID", clientID)
		return false, nil
	}

	if waitTime > 0 {
		lb.log.Debug("leaky bucket queued", "clientID", clientID, "waitSeconds", waitTime)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Duration(waitTime * float64(time.Second))):
		}
	}

	return true, nil
}
