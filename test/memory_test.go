package storage

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStorage_AllowTokenBucket(t *testing.T) {
	t.Run("allows up to capacity", func(t *testing.T) {
		m := NewMemoryStorage()
		for i := 0; i < 5; i++ {
			ok, err := m.AllowTokenBucket(context.Background(), "key1", 5, 1)
			if err != nil || !ok {
				t.Fatalf("request %d: want allowed, got ok=%v err=%v", i, ok, err)
			}
		}
	})

	t.Run("blocks when empty", func(t *testing.T) {
		m := NewMemoryStorage()
		for i := 0; i < 3; i++ {
			m.AllowTokenBucket(context.Background(), "key2", 3, 1) //nolint:errcheck
		}
		ok, err := m.AllowTokenBucket(context.Background(), "key2", 3, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("want denied, got allowed")
		}
	})

	t.Run("refills tokens over time", func(t *testing.T) {
		m := NewMemoryStorage()
		m.AllowTokenBucket(context.Background(), "key3", 1, 10) //nolint:errcheck

		ok, _ := m.AllowTokenBucket(context.Background(), "key3", 1, 10)
		if ok {
			t.Fatal("want denied immediately after exhaustion")
		}

		time.Sleep(120 * time.Millisecond)

		ok, err := m.AllowTokenBucket(context.Background(), "key3", 1, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("want allowed after refill, got denied")
		}
	})
}

func TestMemoryStorage_AllowLeakyBucket(t *testing.T) {
	t.Run("first request returns zero wait", func(t *testing.T) {
		m := NewMemoryStorage()
		wait, err := m.AllowLeakyBucket(context.Background(), "key1", 5, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wait != 0 {
			t.Fatalf("want 0 wait for first request, got %f", wait)
		}
	})

	t.Run("queued request returns positive wait time", func(t *testing.T) {
		m := NewMemoryStorage()
		m.AllowLeakyBucket(context.Background(), "key2", 5, 1) //nolint:errcheck

		wait, err := m.AllowLeakyBucket(context.Background(), "key2", 5, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wait <= 0 {
			t.Fatalf("want positive wait for queued request, got %f", wait)
		}
	})

	t.Run("returns -1 when queue is full", func(t *testing.T) {
		m := NewMemoryStorage()
		// capacity=2, rate=1 → queue fills after 3 requests
		for i := 0; i < 3; i++ {
			m.AllowLeakyBucket(context.Background(), "key3", 2, 1) //nolint:errcheck
		}
		wait, err := m.AllowLeakyBucket(context.Background(), "key3", 2, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wait >= 0 {
			t.Fatalf("want -1 when queue is full, got %f", wait)
		}
	})
}
