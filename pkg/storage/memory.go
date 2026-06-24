package storage

import (
	"context"
	"sync"
	"time"
)

// bucket menyimpan status token dari setiap klien (IP/UserID)
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// MemoryStorage adalah implementasi Storage menggunakan RAM lokal
type MemoryStorage struct {
	mu      sync.Mutex // Pengunci agar thread-safe
	buckets map[string]*bucket
}

// NewMemoryStorage menginisiasi memori baru
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		buckets: make(map[string]*bucket),
	}
}

// AllowTokenBucket berisi rumus matematika Token Bucket
func (m *MemoryStorage) AllowTokenBucket(ctx context.Context, key string, capacity float64, rate float64) (bool, error) {
	m.mu.Lock()         // Kunci RAM agar tidak ada request lain yang menulis data ini
	defer m.mu.Unlock() // Otomatis buka kunci setelah fungsi selesai

	now := time.Now()

	b, exists := m.buckets[key]
	if !exists {
		// Klien baru terdeteksi! Buatkan ember yang penuh
		m.buckets[key] = &bucket{
			tokens:     capacity - 1, // Langsung kurangi 1 token untuk request pertama ini
			lastRefill: now,
		}
		return true, nil // Izinkan masuk
	}

	// Jika klien lama, hitung berapa detik sejak dia terakhir kali request
	timePassed := now.Sub(b.lastRefill).Seconds()
	
	// Rumus isi ulang token: Waktu berlalu * Kecepatan isi ulang
	tokensToAdd := timePassed * rate

	b.tokens += tokensToAdd
	if b.tokens > capacity {
		b.tokens = capacity // Token tidak boleh meluber melebihi kapasitas ember
	}
	b.lastRefill = now

	// Cek apakah tokennya cukup untuk request ini (butuh 1 token)
	if b.tokens >= 1 {
		b.tokens--       // Ambil 1 token
		return true, nil // Izinkan masuk
	}

	return false, nil // Token kurang dari 1, BLOKIR!
}