package limiter

import (
	"context"
	"testing"

	"github.com/JackBerck/fluxguard/pkg/storage"
)

func TestLeakyBucketLimiter_Allow(t *testing.T) {
	t.Run("allows first requests within capacity", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// capacity=5, rate=100 → requests drain very fast so wait time is tiny
		limiter := NewLeakyBucket(store, 5, 100)

		for i := 0; i < 5; i++ {
			ok, err := limiter.Allow(context.Background(), "client1")
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
		limiter := NewLeakyBucket(store, 2, 1)

		bgCtx := context.Background()
		cancelCtx, cancel := context.WithCancel(bgCtx)
		cancel() // pre-cancel so queued allows return right away

		// Seed two requests into the queue using the pre-cancelled ctx so they
		// return immediately with ctx.Err() — we only care about side-effects.
		limiter.Allow(bgCtx, "client2")      //nolint:errcheck — fills slot 0
		limiter.Allow(cancelCtx, "client2")  //nolint:errcheck — fills slot 1
		limiter.Allow(cancelCtx, "client2")  //nolint:errcheck — fills slot 2

		// The fourth call should be rejected because the queue is full.
		ok, err := limiter.Allow(cancelCtx, "client2")
		if err == nil && ok {
			t.Fatal("expected denied when queue is full, got allowed")
		}
		// ok must be false (either denied by queue OR cancelled context)
		if ok {
			t.Fatal("expected denied, got allowed")
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// rate=1 → 1 second between emissions; any queued request waits ~1s
		limiter := NewLeakyBucket(store, 5, 1)

		// Seed one request to push the next into a wait state
		limiter.Allow(context.Background(), "client3") //nolint:errcheck

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		ok, err := limiter.Allow(ctx, "client3")
		if ok {
			t.Fatal("expected denied due to context cancellation, got allowed")
		}
		if err == nil {
			t.Fatal("expected a context error, got nil")
		}
	})
}
