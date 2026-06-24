package test

import (
	"context"
	"testing"
	"time"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func TestTokenBucketLimiter_Allow(t *testing.T) {
	t.Run("allows up to capacity", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		limiter := limiter.NewTokenBucket(store, 5, 1)

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

	t.Run("blocks when bucket is empty", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		limiter := limiter.NewTokenBucket(store, 3, 1)

		for i := 0; i < 3; i++ {
			limiter.Allow(context.Background(), "client2") //nolint:errcheck
		}

		ok, err := limiter.Allow(context.Background(), "client2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected denied, got allowed")
		}
	})

	t.Run("refills tokens over time", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		// rate=10 means a token refills every 100 ms
		limiter := limiter.NewTokenBucket(store, 1, 10)

		limiter.Allow(context.Background(), "client3") //nolint:errcheck

		ok, _ := limiter.Allow(context.Background(), "client3")
		if ok {
			t.Fatal("expected denied immediately after exhaustion")
		}

		time.Sleep(120 * time.Millisecond)

		ok, err := limiter.Allow(context.Background(), "client3")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected allowed after token refill")
		}
	})
}
