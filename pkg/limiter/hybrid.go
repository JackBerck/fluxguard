package limiter

import (
	"context"
	"errors"
	"time"

	"github.com/JackBerck/fluxguard/pkg/storage"
)

// HybridConfig holds the configuration for a [HybridLimiter].
//
// The two stages operate independently on the same store using distinct key
// prefixes, so they can share a single [storage.Storage] instance safely.
type HybridConfig struct {
	// TokenCapacity is the maximum burst size for the token bucket stage; must be > 0.
	TokenCapacity float64

	// TokenRate is the number of tokens refilled per second; must be > 0.
	TokenRate float64

	// LeakyCapacity is the maximum number of requests that may queue in the
	// leaky bucket stage; must be > 0.
	LeakyCapacity float64

	// LeakyRate is the number of requests emitted per second by the leaky
	// bucket stage; must be > 0.
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
	log   Logger
}

// HybridOption configures a [HybridLimiter].
type HybridOption func(*HybridLimiter)

// WithHybridLogger sets the logger used by the limiter.
// Pass nil to disable logging (the default).
func WithHybridLogger(l Logger) HybridOption {
	return func(h *HybridLimiter) { h.log = resolveLogger(l) }
}

// NewHybridLimiter returns a HybridLimiter backed by store with the given cfg.
// All fields in cfg must be positive; an error is returned otherwise.
func NewHybridLimiter(store storage.Storage, cfg HybridConfig, opts ...HybridOption) (*HybridLimiter, error) {
	if store == nil {
		return nil, errors.New("fluxguard: store must not be nil")
	}
	if cfg.TokenCapacity <= 0 {
		return nil, errors.New("fluxguard: TokenCapacity must be greater than zero")
	}
	if cfg.TokenRate <= 0 {
		return nil, errors.New("fluxguard: TokenRate must be greater than zero")
	}
	if cfg.LeakyCapacity <= 0 {
		return nil, errors.New("fluxguard: LeakyCapacity must be greater than zero")
	}
	if cfg.LeakyRate <= 0 {
		return nil, errors.New("fluxguard: LeakyRate must be greater than zero")
	}

	h := &HybridLimiter{store: store, cfg: cfg, log: nopLogger{}}
	for _, opt := range opts {
		opt(h)
	}
	return h, nil
}

// Allow reports whether the request from clientID is permitted.
//
// The clientID is used as a key suffix; the two stages use different prefixes
// ("hybrid:token:" and "hybrid:leaky:") so they never share store keys.
func (h *HybridLimiter) Allow(ctx context.Context, clientID string) (bool, error) {
	// Stage 1: token bucket — burst control.
	ok, err := h.store.AllowTokenBucket(ctx, "hybrid:token:"+clientID, h.cfg.TokenCapacity, h.cfg.TokenRate)
	if err != nil {
		h.log.Error("hybrid token bucket store error", "clientID", clientID, "err", err)
		return false, err
	}
	if !ok {
		h.log.Debug("hybrid denied at token bucket stage", "clientID", clientID)
		return false, nil
	}

	// Stage 2: leaky bucket — rate smoothing.
	waitTime, err := h.store.AllowLeakyBucket(ctx, "hybrid:leaky:"+clientID, h.cfg.LeakyCapacity, h.cfg.LeakyRate)
	if err != nil {
		h.log.Error("hybrid leaky bucket store error", "clientID", clientID, "err", err)
		return false, err
	}
	if waitTime < 0 {
		h.log.Debug("hybrid denied at leaky bucket stage: queue full", "clientID", clientID)
		return false, nil
	}

	if waitTime > 0 {
		h.log.Debug("hybrid queued at leaky bucket stage", "clientID", clientID, "waitSeconds", waitTime)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Duration(waitTime * float64(time.Second))):
		}
	}

	return true, nil
}
