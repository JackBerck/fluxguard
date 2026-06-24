package limiter

import (
	"context"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

type TokenBucketLimiter struct {
	store    storage.Storage // Saving the storage interface to interact with Redis or any other storage backend
	capacity float64 // Maximum number of tokens in the bucket
	rate     float64 // Rate at which tokens are added to the bucket (tokens per second)
}

// NewTokenBucket creates a new instance of TokenBucketLimiter with the specified storage, capacity, and rate.
func NewTokenBucket(store storage.Storage, capacity float64, rate float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		store:    store,
		capacity: capacity,
		rate:     rate,
	}
}

// Allow checks if a request from a client is allowed based on the token bucket algorithm.
func (tb *TokenBucketLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	return tb.store.AllowTokenBucket(ctx, clientID, tb.capacity, tb.rate)
}