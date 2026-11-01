import http from 'k6/http';
import { Writer } from 'k6/x/kafka';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const produceLatency = new Trend('dlq_stress_produce_duration');
const poisonPillSentRate = new Rate('poison_pills_sent');
const dlqQueryDuration = new Trend('dlq_query_duration');
const dlqCaptureVerifiedRate = new Rate('dlq_capture_verified_rate');

const KAFKA_BROKERS = (__ENV.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC = __ENV.KAFKA_TOPIC || 'workflow.events';
const AUDIT_URL = __ENV.AUDIT_URL || 'http://localhost:3002';
const NOTIFICATION_URL = __ENV.NOTIFICATION_URL || 'http://localhost:3001';

const writer = new Writer({
  brokers: KAFKA_BROKERS,
  topic: TOPIC,
  autoCreateTopic: true,
});

export const options = {
  scenarios: {
    // Scenario 1: Inject high-rate poison pill stream directly into Kafka
    poison_pill_burst: {
      executor: 'ramping-arrival-rate',
      startRate: 20,
      timeUnit: '1s',
      stages: [
        { duration: '10s', target: 100 }, // Ramp up poison pill flood
        { duration: '20s', target: 100 }, // Hold high-rate poison injection
        { duration: '5s', target: 0 },
      ],
      preAllocatedVUs: 15,
      maxVUs: 30,
      exec: 'injectPoisonPills',
    },

    // Scenario 2: Active DLQ Capture Verification
    dlq_verifier: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: 3 },
        { duration: '20s', target: 3 },
        { duration: '5s', target: 0 },
      ],
      exec: 'verifyDLQCapture',
    },
  },
  thresholds: {
    'dlq_stress_produce_duration': ['p(95)<50'],
    'poison_pills_sent': ['rate>0.99'],
    'dlq_capture_verified_rate': ['rate>0.95'],
    'dlq_query_duration': ['p(95)<150'],
  },
};

/**
 * 1. Emits 3 types of poison pills to stress DLQ isolation
 */
export function injectPoisonPills() {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;
  const workflowId = `wf-poison-${__VU}-${__ITER}-${Date.now()}`;
  const partitionKey = `${tenantId}:${workflowId}`;

  const poisonType = __ITER % 3;
  let payload = '';

  if (poisonType === 0) {
    payload = `<<<CORRUPT_BYTES_${Date.now()}_NOT_JSON>>>`;
  } else if (poisonType === 1) {
    payload = JSON.stringify({
      id: 'bad-schema',
      source: 'attacker',
      // Missing specversion and datacontenttype
      data: { invalidField: true },
    });
  } else {
    payload = JSON.stringify({
      id: `evt-invalid-${Date.now()}`,
      source: 'workflow-api',
      specversion: '1.0',
      type: 'workflow.unknown_type.v99',
      datacontenttype: 'application/json',
      data: {
        workflowId,
        tenantId,
        currentStatus: 'INVALID_STATUS_CODE',
      },
    });
  }

  const start = Date.now();
  try {
    writer.produce({
      messages: [
        {
          key: partitionKey,
          value: payload,
          headers: {
            'x-tenant-id': tenantId,
            'x-dlq-stress': 'true',
          },
        },
      ],
    });
    produceLatency.add(Date.now() - start);
    poisonPillSentRate.add(true);
  } catch (err) {
    poisonPillSentRate.add(false);
  }

  sleep(0.01);
}

/**
 * 2. Verifies that DLQ records are actively captured in audit-service
 * and that consumers remain alive on /ready
 */
export function verifyDLQCapture() {
  const start = Date.now();
  const res = http.get(`${AUDIT_URL}/api/v1/dlq/messages?limit=20`);
  dlqQueryDuration.add(Date.now() - start);

  const queryOk = check(res, {
    'audit-service DLQ API responds 200': (r) => r.status === 200,
    'DLQ messages array is returned': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.success === true && Array.isArray(body.data.messages);
      } catch (e) {
        return false;
      }
    },
  });

  const auditLive = http.get(`${AUDIT_URL}/ready`);
  const notifLive = http.get(`${NOTIFICATION_URL}/ready`);

  const consumersAlive = check(auditLive, {
    'audit-service consumer is resilient and connected': (r) => r.status === 200,
  }) && check(notifLive, {
    'notification-service consumer is resilient and connected': (r) => r.status === 200,
  });

  dlqCaptureVerifiedRate.add(queryOk && consumersAlive);
  sleep(1);
}

export function teardown() {
  writer.close();

  // Allow 2s for all DLQ messages to settle in database
  sleep(2);
  const finalDlqRes = http.get(`${AUDIT_URL}/api/v1/dlq/messages?limit=10`);
  if (finalDlqRes.status === 200) {
    try {
      const data = JSON.parse(finalDlqRes.body).data;
      console.log(`[Teardown] Total DLQ Messages Captured in Database: ${data.total}`);
    } catch (e) {
      console.log(`[Teardown] Error parsing final DLQ response`);
    }
  }
}
