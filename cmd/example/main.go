// Package main demonstrates all three FluxGuard limiters running side by side.
//
// Endpoints:
//
//	GET /api/data/token  – token bucket only  (capacity=10, rate=2 tok/s)
//	GET /api/data/leaky  – leaky bucket only  (capacity=5,  rate=1 req/s)
//	GET /api/data/hybrid – hybrid two-stage   (token: cap=10 rate=2, leaky: cap=5 rate=1)
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func clientIP(r *http.Request) string {
	return strings.Split(r.RemoteAddr, ":")[0]
}

type allowFunc func(ctx context.Context, clientID string) (bool, error)

func rateLimitMiddleware(allow allowFunc, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		allowed, err := allow(r.Context(), clientIP(r))
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("rate limiter error: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatalf("fluxguard: failed to initialise limiter: %v", err)
	}
	return v
}

func main() {
	logger := limiter.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	store := storage.NewRedisStorage("localhost:6379", "")

	tb := must(limiter.NewTokenBucket(store, 10, 2, limiter.WithTokenBucketLogger(logger)))
	lb := must(limiter.NewLeakyBucket(store, 5, 1, limiter.WithLeakyBucketLogger(logger)))
	hl := must(limiter.NewHybridLimiter(store, limiter.HybridConfig{
		TokenCapacity: 10,
		TokenRate:     2,
		LeakyCapacity: 5,
		LeakyRate:     1,
	}, limiter.WithHybridLogger(logger)))

	handler := func(algo string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "OK [%s] – %s\n", algo, clientIP(r))
		}
	}

	http.HandleFunc("/api/data/token", rateLimitMiddleware(tb.Allow, handler("token")))
	http.HandleFunc("/api/data/leaky", rateLimitMiddleware(lb.Allow, handler("leaky")))
	http.HandleFunc("/api/data/hybrid", rateLimitMiddleware(hl.Allow, handler("hybrid")))

	log.Println("listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}