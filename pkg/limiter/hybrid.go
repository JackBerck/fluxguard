package limiter

import (
	"context"
	"time"

	"github.com/JackBerck/fluxguard/pkg/storage"
)

// HybridConfig holds the configuration for a [HybridLimiter].
//
// The two stages operate independently on the same store using distinct key
// prefixes, so they can share a single [storage.Storage] instance safely.
type HybridConfig struct {
	// TokenCapacity is the maximum burst size for the token bucket stage.
	TokenCapacity float64

	// TokenRate is the number of tokens refilled per second.
	TokenRate float64

	// LeakyCapacity is the maximum number of requests that may queue in the
	// leaky bucket stage.
	LeakyCapacity float64

	// LeakyRate is the number of requests emitted per second by the leaky
	// bucket stage.
	LeakyRate float64
}

// HybridLimiter combines the token bucket and leaky bucket algorithms into a
// two-stage pipeline.
//
// Each incoming request must satisfy both stages in sequence:
//
//  1. Token Bucket – absorbs bursts up to [HybridConfig.TokenCapacity].
//     A request that exhausts the token supply is rejected immediately.
//
//  2. Leaky Bucket – smooths the traffic that passed stage one by queuing it
//     and emitting at a constant rate. A request is rejected when the queue
//     is full; otherwise the calling goroutine sleeps until its turn arrives.
//     Context cancellation during the sleep unblocks the goroutine cleanly.
//
// HybridLimiter is safe for concurrent use by multiple goroutines.
type HybridLimiter struct {
	store storage.Storage
	cfg   HybridConfig
}

// NewHybridLimiter returns a HybridLimiter backed by store with the given cfg.
func NewHybridLimiter(store storage.Storage, cfg HybridConfig) *HybridLimiter {
	return &HybridLimiter{store: store, cfg: cfg}
}

// Allow reports whether the request from clientID is permitted.
//
// The clientID is used as a key suffix; the two stages use different prefixes
// ("hybrid:token:" and "hybrid:leaky:") so they never share Redis keys.
func (h *HybridLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	// Stage 1: token bucket — burst control.
	tokenKey := "hybrid:token:" + clientID
	ok, err := h.store.AllowTokenBucket(ctx, tokenKey, h.cfg.TokenCapacity, h.cfg.TokenRate)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	// Stage 2: leaky bucket — rate smoothing.
	leakyKey := "hybrid:leaky:" + clientID
	waitTime, err := h.store.AllowLeakyBucket(ctx, leakyKey, h.cfg.LeakyCapacity, h.cfg.LeakyRate)
	if err != nil {
		return false, err
	}
	if waitTime < 0 {
		return false, nil
	}

	if waitTime > 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Duration(waitTime * float64(time.Second))):
		}
	}

	return true, nil
}
