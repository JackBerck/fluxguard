package limiter

import (
	"context"

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
}

// NewTokenBucket returns a TokenBucketLimiter backed by store.
//
//   - capacity is the maximum number of tokens (burst size).
//   - rate is the number of tokens added per second.
func NewTokenBucket(store storage.Storage, capacity, rate float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		store:    store,
		capacity: capacity,
		rate:     rate,
	}
}

// Allow reports whether the request from clientID is permitted.
// It uses the "token:" key prefix internally to avoid conflicts with other
// limiter types sharing the same store.
func (tb *TokenBucketLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	return tb.store.AllowTokenBucket(ctx, "token:"+clientID, tb.capacity, tb.rate)
}