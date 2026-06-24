package storage

import (
	"context"
	"sync"
	"time"
)

// tokenBucketEntry holds the token count and the last refill timestamp for a
// single key in the in-memory token bucket store.
type tokenBucketEntry struct {
	tokens     float64
	lastRefill time.Time
}

// leakyBucketEntry holds the last scheduled emission time for a single key in
// the in-memory leaky bucket store.
type leakyBucketEntry struct {
	lastEmission time.Time
}

// MemoryStorage is an in-process implementation of [Storage] backed by a
// Go map protected by a mutex. It is suitable for single-instance deployments
// and testing. For distributed deployments use [RedisStorage].
type MemoryStorage struct {
	mu           sync.Mutex
	tokenBuckets map[string]*tokenBucketEntry
	leakyBuckets map[string]*leakyBucketEntry
}

// NewMemoryStorage returns a new MemoryStorage ready for use.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tokenBuckets: make(map[string]*tokenBucketEntry),
		leakyBuckets: make(map[string]*leakyBucketEntry),
	}
}

// AllowTokenBucket implements [Storage] using an in-memory token bucket.
func (m *MemoryStorage) AllowTokenBucket(_ context.Context, key string, capacity, rate float64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry, ok := m.tokenBuckets[key]
	if !ok {
		m.tokenBuckets[key] = &tokenBucketEntry{
			tokens:     capacity - 1,
			lastRefill: now,
		}
		return true, nil
	}

	elapsed := now.Sub(entry.lastRefill).Seconds()
	entry.tokens = min(entry.tokens+elapsed*rate, capacity)
	entry.lastRefill = now

	if entry.tokens >= 1 {
		entry.tokens--
		return true, nil
	}
	return false, nil
}

// AllowLeakyBucket implements [Storage] using an in-memory leaky bucket.
// It returns the wait time in seconds (≥ 0) or -1 if the queue is full.
func (m *MemoryStorage) AllowLeakyBucket(_ context.Context, key string, capacity, rate float64) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	emissionInterval := time.Duration(float64(time.Second) / rate)

	entry, ok := m.leakyBuckets[key]
	if !ok {
		entry = &leakyBucketEntry{lastEmission: now}
		m.leakyBuckets[key] = entry
	}

	base := entry.lastEmission
	if now.After(base) {
		base = now
	}
	nextEmission := base.Add(emissionInterval)

	queueSize := nextEmission.Sub(now).Seconds() / emissionInterval.Seconds()
	if queueSize > capacity {
		return -1, nil
	}

	entry.lastEmission = nextEmission

	waitTime := nextEmission.Sub(now).Seconds() - emissionInterval.Seconds()
	if waitTime < 0 {
		waitTime = 0
	}
	return waitTime, nil
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}