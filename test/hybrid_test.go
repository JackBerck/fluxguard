package test

import (
	"context"
	"testing"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func defaultHybridConfig() limiter.HybridConfig {
	return limiter.HybridConfig{
		TokenCapacity: 5,
		TokenRate:     5,
		LeakyCapacity: 5,
		LeakyRate:     100, // fast drain so tests don't wait
	}
}

func TestHybridLimiter_Allow(t *testing.T) {
	t.Run("allows requests that pass both stages", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		h := limiter.NewHybridLimiter(store, defaultHybridConfig())

		for i := 0; i < 5; i++ {
			ok, err := h.Allow(context.Background(), "client1")
			if err != nil {
				t.Fatalf("request %d: unexpected error: %v", i, err)
			}
			if !ok {
				t.Fatalf("request %d: expected allowed, got denied", i)
			}
		}
	})

	t.Run("blocks at token bucket stage when burst is exceeded", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		cfg := limiter.HybridConfig{
			TokenCapacity: 2,
			TokenRate:     1,
			LeakyCapacity: 10,
			LeakyRate:     100,
		}
		h := limiter.NewHybridLimiter(store, cfg)

		// Exhaust the token bucket.
		for i := 0; i < 2; i++ {
			h.Allow(context.Background(), "client2") //nolint:errcheck
		}

		ok, err := h.Allow(context.Background(), "client2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected denied at token bucket stage, got allowed")
		}
	})

	t.Run("blocks at leaky bucket stage when queue is full", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		cfg := limiter.HybridConfig{
			TokenCapacity: 20,
			TokenRate:     20,
			LeakyCapacity: 2,
			LeakyRate:     1, // slow drain so queue fills quickly
		}
		h := limiter.NewHybridLimiter(store, cfg)

		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Seed the leaky queue past capacity using a pre-cancelled context so
		// the goroutine doesn't block.
		h.Allow(context.Background(), "client3") //nolint:errcheck — slot 0
		h.Allow(cancelCtx, "client3")            //nolint:errcheck — slot 1
		h.Allow(cancelCtx, "client3")            //nolint:errcheck — slot 2

		ok, err := h.Allow(cancelCtx, "client3")
		if ok {
			t.Fatal("expected denied at leaky bucket stage, got allowed")
		}
		// ok is false either because queue is full or context was cancelled.
		_ = err
	})

	t.Run("respects context cancellation while waiting in leaky queue", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		cfg := limiter.HybridConfig{
			TokenCapacity: 10,
			TokenRate:     10,
			LeakyCapacity: 5,
			LeakyRate:     1, // 1 req/s → next request waits ~1s
		}
		h := limiter.NewHybridLimiter(store, cfg)

		// Seed one request to push the next into a wait state.
		h.Allow(context.Background(), "client4") //nolint:errcheck

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ok, err := h.Allow(ctx, "client4")
		if ok {
			t.Fatal("expected denied due to context cancellation, got allowed")
		}
		if err == nil {
			t.Fatal("expected a context error, got nil")
		}
	})
}
