import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Membuat metrik kustom untuk memantau seberapa banyak request yang sukses dan diblokir
const successRate = new Rate('successful_requests_200');
const blockedRate = new Rate('blocked_requests_429');

export const options = {
    // Skenario simulasi serangan:
    stages: [
        { duration: '5s', target: 50 },  // Pemanasan: Naikkan hingga 50 user bersamaan dalam 5 detik
        { duration: '10s', target: 50 }, // Bertahan: Tahan 50 user tersebut selama 10 detik (Spamming)
        { duration: '5s', target: 0 },   // Pendinginan: Turunkan user kembali ke 0
    ],
    thresholds: {
        // Kita bisa menetapkan target (opsional). Misalnya: 90% request yang diblokir harus merespons < 50ms
        http_req_duration: ['p(90)<50'], 
    },
};

export default function () {
    // URL API Gateway yang sedang berjalan (Pastikan main.go sedang running)
    const url = 'http://localhost:8080/api/data';
    
    // Melakukan HTTP GET Request
    const res = http.get(url);

    // Mengecek dan mencatat status HTTP
    check(res, {
        'status is 200 (OK)': (r) => r.status === 200,
        'status is 429 (Blocked/Rate Limited)': (r) => r.status === 429,
    });

    // Menambahkan data ke metrik kustom kita untuk ditampilkan di hasil akhir
    successRate.add(res.status === 200);
    blockedRate.add(res.status === 429);

    // Jeda antar request untuk setiap virtual user (simulasi user mengetik/ngeklik)
    // Dibuat sangat kecil (0.1 detik) agar sifatnya seperti serangan / traffic membludak
    sleep(0.1); 
}