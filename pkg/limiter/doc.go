// Package limiter provides composable HTTP rate limiters backed by a pluggable
// [storage.Storage] interface. Available algorithms:
//
//   - [TokenBucketLimiter] – allows bursts up to a configured capacity, then
//     refills at a steady rate.
//   - [LeakyBucketLimiter] – smooths traffic by queuing requests and releasing
//     them at a constant rate; rejects requests when the queue is full.
package limiter
