package limiter

import (
	"context"
	"errors"

	"github.com/JackBerck/fluxguard/pkg/storage"
)

// TokenBucketLimiter limits requests using the token bucket algorithm.
// Tokens accumulate at a fixed rate up to a maximum capacity; each allowed
// request consumes one token. Excess requests are rejected immediately.
//
// TokenBucketLimiter is safe for concurrent use by multiple goroutines.
type TokenBucketLimiter struct {
	store    storage.Storage
	capacity float64
	rate     float64
	log      Logger
}

// TokenBucketOption configures a [TokenBucketLimiter].
type TokenBucketOption func(*TokenBucketLimiter)

// WithTokenBucketLogger sets the logger used by the limiter.
// Pass nil to disable logging (the default).
func WithTokenBucketLogger(l Logger) TokenBucketOption {
	return func(tb *TokenBucketLimiter) { tb.log = resolveLogger(l) }
}

// NewTokenBucket returns a TokenBucketLimiter backed by store.
//
//   - capacity is the maximum number of tokens (burst size); must be > 0.
//   - rate is the number of tokens added per second; must be > 0.
func NewTokenBucket(store storage.Storage, capacity, rate float64, opts ...TokenBucketOption) (*TokenBucketLimiter, error) {
	if store == nil {
		return nil, errors.New("fluxguard: store must not be nil")
	}
	if capacity <= 0 {
		return nil, errors.New("fluxguard: capacity must be greater than zero")
	}
	if rate <= 0 {
		return nil, errors.New("fluxguard: rate must be greater than zero")
	}

	tb := &TokenBucketLimiter{
		store:    store,
		capacity: capacity,
		rate:     rate,
		log:      nopLogger{},
	}
	for _, opt := range opts {
		opt(tb)
	}
	return tb, nil
}

// Allow reports whether the request from clientID is permitted.
// It uses the "token:" key prefix internally to avoid conflicts with other
// limiter types sharing the same store.
func (tb *TokenBucketLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	ok, err := tb.store.AllowTokenBucket(ctx, "token:"+clientID, tb.capacity, tb.rate)
	if err != nil {
		tb.log.Error("token bucket store error", "clientID", clientID, "err", err)
		return false, err
	}
	if !ok {
		tb.log.Debug("token bucket denied", "clientID", clientID)
	}
	return ok, nil
}