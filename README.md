<p align="center">
  <img src="logo.webp" alt="FluxGuard" width="180" />
</p>

<h1 align="center">FluxGuard</h1>

<p align="center">
  A composable, backend-agnostic rate-limiting library for Go.<br/>
  Combines Token Bucket and Leaky Bucket algorithms into a flexible, production-ready toolkit.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/JackBerck/fluxguard"><img src="https://pkg.go.dev/badge/github.com/JackBerck/fluxguard.svg" alt="Go Reference"></a>
  <a href="https://github.com/JackBerck/fluxguard/releases"><img src="https://img.shields.io/github/v/tag/JackBerck/fluxguard?label=version" alt="Latest version"></a>
  <img src="https://img.shields.io/badge/go-%3E%3D1.21-blue" alt="Go version">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

---

## What is FluxGuard?

FluxGuard is a Go rate-limiting library designed with library consumers in mind:

- **Zero stdout output** — the library never writes to the terminal unless you explicitly provide a logger.
- **Backend-agnostic** — plug in Redis for distributed deployments or the built-in in-memory store for single-instance services and tests.
- **Safe constructors** — every constructor validates its inputs and returns an `error` instead of panicking.
- **Composable** — use each algorithm independently or combine them via `HybridLimiter`.

---

## Algorithms

### Token Bucket
Allows up to `capacity` requests in a burst, then refills at `rate` tokens per second. Ideal for APIs that need to tolerate short bursts while enforcing an average throughput limit.

```
Tokens ──── refill (rate/s) ────► bucket (cap N)
                                       │
                          request ─────┤ token available? → allow
                                       │ no token?         → deny (429)
```

### Leaky Bucket
Queues incoming requests and releases them at a constant `rate`. Excess requests beyond `capacity` are rejected immediately. Ideal for smoothing bursty traffic into a steady output stream.

```
Request ──► queue (cap N) ──── emit (rate/s) ──► handler
                  │ full?
                  └──► deny (429)
```

### Hybrid (Token Bucket + Leaky Bucket)
A two-stage pipeline that provides both burst tolerance and output smoothing in a single call.

```
Request ──► [Token Bucket] ──pass──► [Leaky Bucket] ──pass──► handler
                 │ deny                    │ deny
                 └──► 429                 └──► 429
```

---

## Installation

Requires Go 1.21 or later (for `log/slog`).

```bash
go get github.com/JackBerck/fluxguard@latest
```

---

## Quick Start

### Token Bucket

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/JackBerck/fluxguard/pkg/limiter"
    "github.com/JackBerck/fluxguard/pkg/storage"
)

func main() {
    store := storage.NewRedisStorage("localhost:6379", "")

    // capacity=10 tokens, refill rate=2 tokens/second
    tb, err := limiter.NewTokenBucket(store, 10, 2)
    if err != nil {
        log.Fatal(err)
    }

    ok, err := tb.Allow(context.Background(), "user-123")
    if err != nil {
        log.Fatal(err)
    }
    if !ok {
        fmt.Println("rate limited")
    }
}
```

### Leaky Bucket

```go
// queue capacity=5, emit rate=1 request/second
lb, err := limiter.NewLeakyBucket(store, 5, 1)
if err != nil {
    log.Fatal(err)
}

ok, err := lb.Allow(r.Context(), clientIP)
```

### Hybrid Limiter

```go
hl, err := limiter.NewHybridLimiter(store, limiter.HybridConfig{
    TokenCapacity: 10, // burst up to 10
    TokenRate:     2,  // refill 2 tokens/s
    LeakyCapacity: 5,  // queue up to 5
    LeakyRate:     1,  // emit 1 req/s
})
if err != nil {
    log.Fatal(err)
}

ok, err := hl.Allow(r.Context(), clientIP)
```

### Opt-in Logging

FluxGuard is silent by default. To enable structured logging, pass any value that satisfies `limiter.Logger` — a `*slog.Logger` works out of the box via `limiter.NewSlogLogger`:

```go
import "log/slog"

logger := limiter.NewSlogLogger(slog.Default())

tb, err := limiter.NewTokenBucket(store, 10, 2,
    limiter.WithTokenBucketLogger(logger),
)
```

You can also implement `limiter.Logger` yourself to integrate with any logging framework (Zap, Zerolog, etc.).

### In-Memory Store (for testing)

```go
store := storage.NewMemoryStorage()
tb, _ := limiter.NewTokenBucket(store, 5, 1)
```

---

## HTTP Middleware Example

```go
func rateLimitMiddleware(allow func(context.Context, string) (bool, error)) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            ok, err := allow(r.Context(), r.RemoteAddr)
            if err != nil && err != context.Canceled {
                http.Error(w, "Internal Server Error", http.StatusInternalServerError)
                return
            }
            if !ok {
                http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
                return
            }
            next.ServeHTTP(w, r)
        })
    }
}

http.Handle("/api/data", rateLimitMiddleware(hl.Allow)(myHandler))
```

---

## Storage Backends

| Backend | Type | Use case |
|---|---|---|
| `storage.NewMemoryStorage()` | In-process | Single instance, testing |
| `storage.NewRedisStorage(addr, password)` | Redis (distributed) | Multi-instance, production |

Implement the `storage.Storage` interface to use any other backend (Memcached, DynamoDB, etc.).

---

## Technologies

| Technology | Role |
|---|---|
| [Go](https://go.dev) | Core language |
| [Redis](https://redis.io) + [go-redis](https://github.com/redis/go-redis) | Distributed state via atomic Lua scripts |
| [log/slog](https://pkg.go.dev/log/slog) | Structured logging adapter |
| [k6](https://k6.io) | Load testing (`k6/` directory) |

---

## Project Structure

```
fluxguard/
├── pkg/
│   ├── limiter/          # Rate limiter implementations
│   │   ├── token.go      # TokenBucketLimiter
│   │   ├── leaky.go      # LeakyBucketLimiter
│   │   ├── hybrid.go     # HybridLimiter + HybridConfig
│   │   └── logger.go     # Logger interface + SlogLogger adapter
│   └── storage/          # Storage interface and backends
│       ├── interface.go
│       ├── memory.go     # In-memory backend
│       └── redis.go      # Redis backend (atomic Lua scripts)
├── test/                 # Integration-style tests (package test)
├── k6/                   # k6 load-test scripts
├── cmd/example/          # Runnable demo server
└── .private/             # Local notes (git-ignored)
    └── VERSIONING.md
```

---

## Running Tests

```bash
go test ./...
```

> The `test/` package uses `storage.MemoryStorage` so no Redis instance is required.

---

## Running the Example Server

Start Redis locally, then:

```bash
go run ./cmd/example
```

Endpoints available at `http://localhost:8080`:

| Endpoint | Algorithm |
|---|---|
| `/api/data/token` | Token Bucket |
| `/api/data/leaky` | Leaky Bucket |
| `/api/data/hybrid` | Hybrid |

Run the k6 load test:

```bash
k6 run k6/loadtest_hybrid.js
```
