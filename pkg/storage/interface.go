package storage

import "context"

// Storage is implemented by any backend that can persist rate-limiting state.
// Provided implementations include [RedisStorage] and [MemoryStorage].
type Storage interface {
	// AllowTokenBucket reports whether a request identified by key is allowed
	// under the token bucket algorithm with the given capacity and refill rate
	// (tokens per second).
	AllowTokenBucket(ctx context.Context, key string, capacity float64, rate float64) (bool, error)

	// AllowLeakyBucket schedules a request identified by key in the leaky
	// bucket queue. It returns the wait duration in seconds before the request
	// may proceed, or a negative value if the queue is full and the request
	// must be rejected.
	AllowLeakyBucket(ctx context.Context, key string, capacity float64, rate float64) (float64, error)
}
