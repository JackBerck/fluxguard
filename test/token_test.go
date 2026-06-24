package test

import (
	"context"
	"testing"
	"time"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func mustNewTokenBucket(t *testing.T, store *storage.MemoryStorage, capacity, rate float64) *limiter.TokenBucketLimiter {
	t.Helper()
	l, err := limiter.NewTokenBucket(store, capacity, rate)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return l
}

func TestTokenBucketLimiter_Allow(t *testing.T) {
	t.Run("allows up to capacity", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		lb := mustNewTokenBucket(t, store, 5, 1)

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

	t.Run("blocks when bucket is empty", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		lb := mustNewTokenBucket(t, store, 3, 1)

		for i := 0; i < 3; i++ {
			lb.Allow(context.Background(), "client2") //nolint:errcheck
		}

		ok, err := lb.Allow(context.Background(), "client2")
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
		lb := mustNewTokenBucket(t, store, 1, 10)

		lb.Allow(context.Background(), "client3") //nolint:errcheck

		ok, _ := lb.Allow(context.Background(), "client3")
		if ok {
			t.Fatal("expected denied immediately after exhaustion")
		}

		time.Sleep(120 * time.Millisecond)

		ok, err := lb.Allow(context.Background(), "client3")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected allowed after token refill")
		}
	})
}

func TestNewTokenBucket_Validation(t *testing.T) {
	store := storage.NewMemoryStorage()

	cases := []struct {
		name     string
		capacity float64
		rate     float64
	}{
		{"zero capacity", 0, 1},
		{"negative capacity", -1, 1},
		{"zero rate", 5, 0},
		{"negative rate", 5, -2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := limiter.NewTokenBucket(store, tc.capacity, tc.rate)
			if err == nil {
				t.Fatal("expected error for invalid config, got nil")
			}
		})
	}

	t.Run("nil store", func(t *testing.T) {
		_, err := limiter.NewTokenBucket(nil, 5, 1)
		if err == nil {
			t.Fatal("expected error for nil store, got nil")
		}
	})
}
