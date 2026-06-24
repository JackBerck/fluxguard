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
	// ========================================================
	// PERUBAHAN HANYA DI BARIS INI: Switch ke Redis!
	// ========================================================
	redisStore := storage.NewRedisStorage("localhost:6379", "")

	// Cek koneksi ke Docker Redis (Biar aman)
	_, err := redisStore.AllowTokenBucket(context.Background(), "test_ping", 1, 1)
	if err != nil {
		log.Fatalf("❌ Gagal connect ke Redis! Pastikan Docker jalan. Error: %v", err)
	}
	fmt.Println("🔗 Connected to Redis Server successfully!")

	// 2. Konfigurasi Token Bucket (Kapasitas 5, Refill 1/detik)
	// Perhatikan: Kita sekarang melempar redisStore, bukan memStore!
	tb := limiter.NewTokenBucket(redisStore, 5, 1)

	// 3. Setup API Gateway
	http.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		ip := strings.Split(r.RemoteAddr, ":")[0]

		allowed, err := tb.Allow(context.Background(), ip)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		
		if !allowed {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, "🚫 [429] TOO MANY REQUESTS! Fluxguard (Redis) memblokir IP: %s\n", ip)
			fmt.Println("BLOCKED ->", ip)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "✅ [200] OK! Request berhasil dari IP: %s\n", ip)
		fmt.Println("ALLOWED ->", ip)
	})

	fmt.Println("🚀 Server Fluxguard (Distributed Mode) berjalan di http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}