package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage is a distributed implementation of [Storage] backed by Redis.
// All operations are executed as atomic Lua scripts to prevent race conditions
// in multi-instance deployments.
type RedisStorage struct {
	client *redis.Client
}

// NewRedisStorage returns a RedisStorage connected to the given address.
// Pass an empty string for password if Redis is not password-protected.
func NewRedisStorage(addr, password string) *RedisStorage {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &RedisStorage{client: client}
}

// tokenBucketScript atomically refills and consumes a token from a Redis Hash.
// Returns 1 if the request is allowed, 0 if the bucket is empty.
const tokenBucketScript = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local data       = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens or not last_refill then
    redis.call('HMSET', key, 'tokens', capacity - 1, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1
end

local elapsed      = now - last_refill
local tokens_to_add = elapsed * rate
tokens = math.min(tokens + tokens_to_add, capacity)

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, 3600)
return 0
`

// leakyBucketScript schedules a request in a virtual queue stored as a single
// Redis String. Returns the wait time in seconds (≥ 0) or -1 when the queue
// is at capacity.
const leakyBucketScript = `
local key               = KEYS[1]
local capacity          = tonumber(ARGV[1])
local rate              = tonumber(ARGV[2])
local now               = tonumber(ARGV[3])
local emission_interval = 1.0 / rate

local last_emission = tonumber(redis.call('GET', key)) or now
local next_emission = math.max(now, last_emission) + emission_interval
local queue_size    = (next_emission - now) / emission_interval

if queue_size > capacity then
    return -1
end

redis.call('SET',    key, next_emission)
redis.call('EXPIRE', key, 3600)

local wait_time = next_emission - now - emission_interval
if wait_time < 0 then wait_time = 0 end
return wait_time
`

// AllowTokenBucket implements [Storage] using a Redis-backed token bucket.
func (r *RedisStorage) AllowTokenBucket(ctx context.Context, key string, capacity, rate float64) (bool, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	result, err := r.client.Eval(ctx, tokenBucketScript, []string{key}, capacity, rate, now).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}

// AllowLeakyBucket implements [Storage] using a Redis-backed leaky bucket.
// It returns the wait time in seconds (≥ 0) or -1 if the queue is full.
func (r *RedisStorage) AllowLeakyBucket(ctx context.Context, key string, capacity, rate float64) (float64, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	result, err := r.client.Eval(ctx, leakyBucketScript, []string{key}, capacity, rate, now).Result()
	if err != nil {
		return 0, err
	}

	switch v := result.(type) {
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		var parsed float64
		fmt.Sscanf(v, "%f", &parsed)
		return parsed, nil
	default:
		return 0, nil
	}
}
