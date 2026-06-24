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

func mustNewHybridLimiter(t *testing.T, store *storage.MemoryStorage, cfg limiter.HybridConfig) *limiter.HybridLimiter {
	t.Helper()
	h, err := limiter.NewHybridLimiter(store, cfg)
	if err != nil {
		t.Fatalf("NewHybridLimiter: %v", err)
	}
	return h
}

func TestHybridLimiter_Allow(t *testing.T) {
	t.Run("allows requests that pass both stages", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		h := mustNewHybridLimiter(t, store, defaultHybridConfig())

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
		h := mustNewHybridLimiter(t, store, cfg)

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
			LeakyRate:     1,
		}
		h := mustNewHybridLimiter(t, store, cfg)

		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		h.Allow(context.Background(), "client3") //nolint:errcheck — slot 0
		h.Allow(cancelCtx, "client3")            //nolint:errcheck — slot 1
		h.Allow(cancelCtx, "client3")            //nolint:errcheck — slot 2

		ok, err := h.Allow(cancelCtx, "client3")
		if ok {
			t.Fatal("expected denied at leaky bucket stage, got allowed")
		}
		_ = err
	})

	t.Run("respects context cancellation while waiting in leaky queue", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		cfg := limiter.HybridConfig{
			TokenCapacity: 10,
			TokenRate:     10,
			LeakyCapacity: 5,
			LeakyRate:     1,
		}
		h := mustNewHybridLimiter(t, store, cfg)

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

func TestNewHybridLimiter_Validation(t *testing.T) {
	store := storage.NewMemoryStorage()
	valid := defaultHybridConfig()

	cases := []struct {
		name string
		cfg  limiter.HybridConfig
	}{
		{"zero TokenCapacity", limiter.HybridConfig{TokenCapacity: 0, TokenRate: valid.TokenRate, LeakyCapacity: valid.LeakyCapacity, LeakyRate: valid.LeakyRate}},
		{"zero TokenRate", limiter.HybridConfig{TokenCapacity: valid.TokenCapacity, TokenRate: 0, LeakyCapacity: valid.LeakyCapacity, LeakyRate: valid.LeakyRate}},
		{"zero LeakyCapacity", limiter.HybridConfig{TokenCapacity: valid.TokenCapacity, TokenRate: valid.TokenRate, LeakyCapacity: 0, LeakyRate: valid.LeakyRate}},
		{"zero LeakyRate", limiter.HybridConfig{TokenCapacity: valid.TokenCapacity, TokenRate: valid.TokenRate, LeakyCapacity: valid.LeakyCapacity, LeakyRate: 0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := limiter.NewHybridLimiter(store, tc.cfg)
			if err == nil {
				t.Fatal("expected error for invalid config, got nil")
			}
		})
	}

	t.Run("nil store", func(t *testing.T) {
		_, err := limiter.NewHybridLimiter(nil, valid)
		if err == nil {
			t.Fatal("expected error for nil store, got nil")
		}
	})
}
