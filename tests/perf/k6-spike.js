import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const spikeLatency = new Trend('spike_req_duration');
const spikeSuccessRate = new Rate('spike_success_rate');
const spikeThrottleRate = new Rate('spike_throttle_429_rate');

export const options = {
  stages: [
    { duration: '5s', target: 10 },   // Warm-up baseline
    { duration: '10s', target: 150 }, // Rapid spike surge to 150 VUs
    { duration: '30s', target: 150 }, // Sustained spike peak
    { duration: '10s', target: 10 },  // Quick scale-down recovery
    { duration: '5s', target: 0 },    // Cool-down
  ],
  thresholds: {
    // Under high spike, error rate (5xx) must remain below 5%
    'http_req_failed': ['rate<0.05'],
    'spike_success_rate': ['rate>0.95'],
    'spike_req_duration': ['p(95)<500', 'p(99)<1000'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

export default function () {
  const tenantId = `tenant-${Math.floor(Math.random() * 10) + 1}`;
  const userId = `user-spike-${__VU}`;

  const headers = {
    'Content-Type': 'application/json',
    'x-tenant-id': tenantId,
    'x-user-id': userId,
    'x-user-name': `Spike Tester ${__VU}`,
    'idempotency-key': `spike-${__VU}-${__ITER}-${Date.now()}`,
  };

  const payload = JSON.stringify({
    type: 'EXPENSE',
    title: `Spike Surge Item - VU ${__VU} - ${Date.now()}`,
    amount: Math.floor(Math.random() * 5000) + 100,
    currency: 'USD',
    description: 'Sudden traffic burst spike test',
  });

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/workflows`, payload, {
    headers,
    tags: { test_type: 'spike' },
  });
  spikeLatency.add(Date.now() - start);

  const isSuccess = res.status === 201;
  const isThrottled = res.status === 429;
  spikeSuccessRate.add(isSuccess || isThrottled);
  spikeThrottleRate.add(isThrottled);

  check(res, {
    'spike response is 201 or throttled 429 (graceful)': (r) => r.status === 201 || r.status === 429,
    'spike response is not 5xx internal error': (r) => r.status < 500,
  });

  // Short pause to maintain pressure during the surge
  sleep(0.2);
}
