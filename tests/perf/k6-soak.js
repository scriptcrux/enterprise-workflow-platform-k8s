import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const soakReqDuration = new Trend('soak_req_duration');
const soakErrorRate = new Rate('soak_error_rate');

export const options = {
  stages: [
    { duration: '30s', target: 25 },  // Ramp-up to steady load
    { duration: '10m', target: 25 },  // Sustained soak window (10 minutes)
    { duration: '30s', target: 0 },   // Clean ramp-down
  ],
  thresholds: {
    // Over a 10+ minute soak, error rate must stay below 0.5% and response times must not degrade
    'http_req_failed': ['rate<0.005'],
    'soak_req_duration': ['p(95)<200', 'p(99)<400'],
    'soak_error_rate': ['rate<0.005'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

export default function () {
  const tenantId = `tenant-${Math.floor(Math.random() * 8) + 1}`;
  const userId = `user-soak-${__VU}`;

  const headers = {
    'Content-Type': 'application/json',
    'x-tenant-id': tenantId,
    'x-user-id': userId,
    'x-user-name': `Soak Worker ${__VU}`,
  };

  // 1. Create Draft
  const createStart = Date.now();
  const createRes = http.post(
    `${BASE_URL}/api/v1/workflows`,
    JSON.stringify({
      type: 'EXPENSE',
      title: `Soak Test Workflow - VU ${__VU} - ${Date.now()}`,
      amount: 250,
      currency: 'USD',
    }),
    {
      headers: {
        ...headers,
        'idempotency-key': `soak-create-${__VU}-${Date.now()}-${Math.random()}`,
      },
      tags: { op: 'soak_create' },
    }
  );
  soakReqDuration.add(Date.now() - createStart);

  let workflowId = null;
  const createdOk = check(createRes, {
    'soak create returns 201': (r) => r.status === 201,
  });

  if (createdOk) {
    try {
      workflowId = JSON.parse(createRes.body).data.id;
    } catch (e) {
      soakErrorRate.add(true);
    }
  } else {
    soakErrorRate.add(true);
  }

  // 2. Submit Workflow
  if (workflowId) {
    const submitStart = Date.now();
    const submitRes = http.post(
      `${BASE_URL}/api/v1/workflows/${workflowId}/submit`,
      JSON.stringify({ comment: 'Soak submission' }),
      {
        headers: {
          ...headers,
          'idempotency-key': `soak-submit-${workflowId}`,
        },
        tags: { op: 'soak_submit' },
      }
    );
    soakReqDuration.add(Date.now() - submitStart);

    check(submitRes, {
      'soak submit returns 200': (r) => r.status === 200,
    });
  }

  // 3. Query list
  const listStart = Date.now();
  const listRes = http.get(`${BASE_URL}/api/v1/workflows?limit=10`, {
    headers,
    tags: { op: 'soak_list' },
  });
  soakReqDuration.add(Date.now() - listStart);

  check(listRes, {
    'soak list returns 200': (r) => r.status === 200,
  });

  // Paced interval between iterations to emulate steady human/worker activity
  sleep(1.5);
}
