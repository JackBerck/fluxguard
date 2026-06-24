// Package main demonstrates FluxGuard's LeakyBucketLimiter in an HTTP server.
//
// Run the server and repeatedly GET /api/data to observe rate limiting in action:
// requests are queued and released at 1 req/s; once the queue (capacity 5) is
// full, excess requests receive HTTP 429 immediately.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/JackBerck/fluxguard/pkg/limiter"
	"github.com/JackBerck/fluxguard/pkg/storage"
)

func main() {
	store := storage.NewRedisStorage("localhost:6379", "")
	lb := limiter.NewLeakyBucket(store, 5, 1)

	http.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		ip := strings.Split(r.RemoteAddr, ":")[0]

		allowed, err := lb.Allow(r.Context(), ip)
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

		fmt.Fprintf(w, "OK – served request from %s\n", ip)
	})

	log.Println("listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}