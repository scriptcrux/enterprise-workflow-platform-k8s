import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const sseConnectDuration = new Trend('sse_connect_duration');
const notificationQueryDuration = new Trend('notification_query_duration');
const sseSuccessRate = new Rate('sse_connection_success');

export const options = {
  scenarios: {
    // Scenario 1: Long-lived SSE Streaming Clients (connecting to /api/v1/stream)
    sse_streaming_subscribers: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 50 },  // Ramp up to 50 active SSE listeners
        { duration: '30s', target: 50 },  // Maintain active streaming subscribers
        { duration: '10s', target: 0 },   // Disconnect and clean up
      ],
      exec: 'runSseClient',
    },

    // Scenario 2: Historical Notifications Polling & Query Traffic
    notification_polling: {
      executor: 'constant-arrival-rate',
      rate: 30, // 30 req/sec query volume
      timeUnit: '1s',
      duration: '50s',
      preAllocatedVUs: 15,
      maxVUs: 30,
      exec: 'runNotificationPolling',
    },
  },

  thresholds: {
    'http_req_failed': ['rate<0.01'],
    'sse_connect_duration': ['p(95)<150', 'p(99)<300'],
    'notification_query_duration': ['p(95)<100', 'p(99)<200'],
    'sse_connection_success': ['rate>0.99'],
  },
};

const NOTIFICATION_URL = __ENV.NOTIFICATION_URL || 'http://localhost:3001';

/**
 * 1. SSE Stream Client Connection Handler
 */
export function runSseClient() {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;
  const userId = `user-sse-${__VU}`;

  const url = `${NOTIFICATION_URL}/api/v1/stream?tenantId=${tenantId}&userId=${userId}`;
  const params = {
    headers: {
      Accept: 'text/event-stream',
      'Cache-Control': 'no-cache',
    },
    timeout: '5s', // Short stream slice for k6 iteration
  };

  const start = Date.now();
  const res = http.get(url, params);
  sseConnectDuration.add(Date.now() - start);

  const isSseHeaderValid =
    res.headers['Content-Type'] &&
    res.headers['Content-Type'].includes('text/event-stream');

  const ok = check(res, {
    'SSE endpoint responds 200': (r) => r.status === 200,
    'SSE Content-Type is text/event-stream': () => !!isSseHeaderValid,
  });

  sseSuccessRate.add(ok);
  sleep(1);
}

/**
 * 2. Notification Query / Polling Handler
 */
export function runNotificationPolling() {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;
  const recipientId = `user-sse-${Math.floor(Math.random() * 50) + 1}`;

  const url = `${NOTIFICATION_URL}/api/v1/notifications?tenantId=${tenantId}&recipientId=${recipientId}`;

  const start = Date.now();
  const res = http.get(url, {
    headers: {
      'Content-Type': 'application/json',
    },
  });
  notificationQueryDuration.add(Date.now() - start);

  check(res, {
    'notifications query status is 200': (r) => r.status === 200,
    'notifications payload is valid array': (r) => {
      try {
        const body = JSON.parse(r.body);
        return Array.isArray(body.data);
      } catch (e) {
        return false;
      }
    },
  });

  sleep(0.5);
}
