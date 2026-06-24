package test

import (
	"context"
	"testing"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func mustNewLeakyBucket(t *testing.T, store *storage.MemoryStorage, capacity, rate float64) *limiter.LeakyBucketLimiter {
	t.Helper()
	l, err := limiter.NewLeakyBucket(store, capacity, rate)
	if err != nil {
		t.Fatalf("NewLeakyBucket: %v", err)
	}
	return l
}

func TestLeakyBucketLimiter_Allow(t *testing.T) {
	t.Run("allows first requests within capacity", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// capacity=5, rate=100 → requests drain very fast so wait time is tiny
		lb := mustNewLeakyBucket(t, store, 5, 100)

		for i := 0; i < 5; i++ {
			ok, err := lb.Allow(context.Background(), "client1")
			if err != nil {
				t.Fatalf("request %d: unexpected error: %v", i, err)
			}
			if !ok {
				t.Fatalf("request %d: expected allowed, got denied", i)
			}
		}
	})

	t.Run("blocks when queue is full", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// capacity=2, rate=1 → emission interval = 1s
		// Requests 1 & 2 are queued (wait ~1s and ~2s); request 3 overflows.
		// We use a cancelled context so the waiting goroutines return immediately.
		lb := mustNewLeakyBucket(t, store, 2, 1)

		bgCtx := context.Background()
		cancelCtx, cancel := context.WithCancel(bgCtx)
		cancel()

		lb.Allow(bgCtx, "client2")     //nolint:errcheck — fills slot 0
		lb.Allow(cancelCtx, "client2") //nolint:errcheck — fills slot 1
		lb.Allow(cancelCtx, "client2") //nolint:errcheck — fills slot 2

		// The fourth call should be rejected because the queue is full.
		ok, err := lb.Allow(cancelCtx, "client2")
		if err == nil && ok {
			t.Fatal("expected denied when queue is full, got allowed")
		}
		if ok {
			t.Fatal("expected denied, got allowed")
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// rate=1 → 1 second between emissions; any queued request waits ~1s
		lb := mustNewLeakyBucket(t, store, 5, 1)

		lb.Allow(context.Background(), "client3") //nolint:errcheck

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ok, err := lb.Allow(ctx, "client3")
		if ok {
			t.Fatal("expected denied due to context cancellation, got allowed")
		}
		if err == nil {
			t.Fatal("expected a context error, got nil")
		}
	})
}

func TestNewLeakyBucket_Validation(t *testing.T) {
	store := storage.NewMemoryStorage()

	cases := []struct {
		name     string
		capacity float64
		rate     float64
	}{
		{"zero capacity", 0, 1},
		{"negative capacity", -5, 1},
		{"zero rate", 5, 0},
		{"negative rate", 5, -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := limiter.NewLeakyBucket(store, tc.capacity, tc.rate)
			if err == nil {
				t.Fatal("expected error for invalid config, got nil")
			}
		})
	}

	t.Run("nil store", func(t *testing.T) {
		_, err := limiter.NewLeakyBucket(nil, 5, 1)
		if err == nil {
			t.Fatal("expected error for nil store, got nil")
		}
	})
}
