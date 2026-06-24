import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const successRate = new Rate('successful_requests_200');
const blockedRate  = new Rate('blocked_requests_429');

export const options = {
    stages: [
        { duration: '5s',  target: 50 },
        { duration: '10s', target: 50 },
        { duration: '5s',  target: 0  },
    ],
    thresholds: {
        // Blocked requests (429) should respond in under 50 ms — the hybrid
        // limiter must reject at the token bucket stage before any I/O wait.
        http_req_duration: ['p(90)<50'],
    },
};

export default function () {
    const res = http.get('http://localhost:8080/api/data/hybrid');

    check(res, {
        'status is 200': (r) => r.status === 200,
        'status is 429': (r) => r.status === 429,
    });

    successRate.add(res.status === 200);
    blockedRate.add(res.status === 429);

    sleep(0.1);
}
