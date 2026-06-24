package storage

import (
	"context"
	// "time"
)

// Storage is an interface that defines methods for interacting with a storage backend, such as Redis, Memcached, or in-memory storage. It provides methods for incrementing counters and evaluating Lua scripts.
type Storage interface {
	AllowTokenBucket(ctx context.Context, key string, capacity float64, rate float64) (bool, error)
	// Increment(ctx context.Context, key string, limit int, window time.Duration) (int, error)             // Increments a counter for a given key and returns the new value. If the counter exceeds the specified limit within the given time window, it should return an error or a specific value indicating that the limit has been reached.
	// EvalLua(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) // Evaluates a Lua script in the storage backend, allowing for atomic operations and complex logic.
}
