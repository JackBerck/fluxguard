package storage

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements the Storage interface using Redis as the backend
type RedisStorage struct {
	client *redis.Client
}

// NewRedisStorage initializes a new RedisStorage instance with the given Redis server address and password.
func NewRedisStorage(addr string, password string) *RedisStorage {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // Leave blank if no password is set
		DB:       0,        // Use default DB
	})

	return &RedisStorage{
		client: client,
	}
}

// Lua script for token bucket algorithm in Redis (executed atomically)
const tokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- Retrieve token count and last refill time from Redis (stored as a Hash)
local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

-- If this is the user's first API request
if not tokens or not last_refill then
    tokens = capacity - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600) -- Clear memory if inactive for 1 hour
    return 1
end

-- For returning users, calculate token refill based on elapsed time
local time_passed = now - last_refill
local tokens_to_add = time_passed * rate

tokens = tokens + tokens_to_add
if tokens > capacity then
    tokens = capacity
end

-- Check if sufficient tokens are available to allow this request
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1 -- ALLOWED (True)
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 0 -- BLOCKED (False)
end
`

// AllowTokenBucket executes the Lua script defined above
func (r *RedisStorage) AllowTokenBucket(ctx context.Context, key string, capacity float64, rate float64) (bool, error) {
    // Get the current time with decimal precision (seconds.microseconds)
    // Ensures token refills are calculated with high accuracy
    now := float64(time.Now().UnixNano()) / 1e9

    // Execute the Lua script on Redis
    result, err := r.client.Eval(ctx, tokenBucketScript, []string{key}, capacity, rate, now).Result()
    if err != nil {
        return false, err
    }

    // Redis Lua returns 1 (int64) if allowed, 0 if denied
    return result.(int64) == 1, nil
}
