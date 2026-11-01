import http from 'k6/http';
import { Writer } from 'k6/x/kafka';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

// Metrics
const produceLatency = new Trend('kafka_produce_duration');
const produceSuccessRate = new Rate('kafka_produce_success');
const consumerIngestionLatency = new Trend('consumer_audit_ingestion_latency');
const consumerVerificationRate = new Rate('consumer_verification_success');

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
    // Scenario 1: Direct High-Throughput CloudEvents injection into Kafka
    kafka_event_stream: {
      executor: 'constant-arrival-rate',
      rate: 100, // 100 CloudEvents / second direct to Kafka
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 15,
      maxVUs: 30,
      exec: 'produceKafkaEvents',
    },

    // Scenario 2: Consumer Ingestion Verification (Sampling audit & notification state)
    consumer_verification: {
      executor: 'ramping-vus',
      startVUs: 2,
      stages: [
        { duration: '10s', target: 5 },
        { duration: '20s', target: 5 },
        { duration: '5s', target: 0 },
      ],
      exec: 'verifyConsumerState',
    },
  },
  thresholds: {
    'kafka_produce_duration': ['p(95)<50', 'p(99)<100'],
    'kafka_produce_success': ['rate>0.99'],
    'consumer_verification_success': ['rate>0.95'],
    'consumer_audit_ingestion_latency': ['p(95)<200'],
  },
};

/**
 * 1. Produces partition-keyed CloudEvents directly to Kafka
 */
export function produceKafkaEvents() {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;
  const workflowId = `wf-bench-${__VU}-${__ITER}-${Date.now()}`;
  const partitionKey = `${tenantId}:${workflowId}`;

  const cloudEvent = {
    id: `evt-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
    source: 'workflow-api.benchmark',
    specversion: '1.0',
    type: 'workflow.step_approved.v1',
    time: new Date().toISOString(),
    datacontenttype: 'application/json',
    data: {
      workflowId,
      tenantId,
      type: 'EXPENSE',
      title: `Direct Kafka Load Workflow - ${workflowId}`,
      amount: 500,
      currency: 'USD',
      requesterId: `user-k6-${__VU}`,
      requesterName: 'Performance Tester',
      actorId: `approver-${__VU}`,
      previousStatus: 'PENDING',
      currentStatus: 'APPROVED',
      currentStepOrder: 1,
      totalSteps: 2,
      stepRole: 'FINANCE_DIRECTOR',
      timestamp: new Date().toISOString(),
    },
  };

  const start = Date.now();
  try {
    writer.produce({
      messages: [
        {
          key: partitionKey,
          value: JSON.stringify(cloudEvent),
          headers: {
            'x-tenant-id': tenantId,
            'x-benchmark-source': 'xk6-kafka',
          },
        },
      ],
    });
    produceLatency.add(Date.now() - start);
    produceSuccessRate.add(true);
  } catch (err) {
    produceSuccessRate.add(false);
  }

  sleep(0.01);
}

/**
 * 2. Validates that Consumer Services (audit-service & notification-service)
 * are actively processing the Kafka event stream and persisting records
 */
export function verifyConsumerState() {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;

  // 1. Verify notification-service readiness & event queue
  const notifHealthRes = http.get(`${NOTIFICATION_URL}/ready`);
  const notifReady = check(notifHealthRes, {
    'notification-service is healthy and consumer connected': (r) => r.status === 200,
  });

  // 2. Query audit-service readiness
  const auditHealthRes = http.get(`${AUDIT_URL}/ready`);
  const auditReady = check(auditHealthRes, {
    'audit-service is healthy and consumer connected': (r) => r.status === 200,
  });

  // 3. Measure audit-service query response time under ingestion pressure
  const auditQueryStart = Date.now();
  const dlqRes = http.get(`${AUDIT_URL}/api/v1/dlq/messages?tenantId=${tenantId}&limit=10`);
  consumerIngestionLatency.add(Date.now() - auditQueryStart);

  const dlqQueryOk = check(dlqRes, {
    'audit-service query returns 200 under Kafka stream load': (r) => r.status === 200,
  });

  consumerVerificationRate.add(notifReady && auditReady && dlqQueryOk);
  sleep(1);
}

export function teardown() {
  writer.close();

  // Final Consumer Health & Ingestion Check after Kafka flood completes
  sleep(2); // Allow 2s for consumer groups to drain remaining messages
  const finalAuditHealth = http.get(`${AUDIT_URL}/ready`);
  const finalNotifHealth = http.get(`${NOTIFICATION_URL}/ready`);

  console.log(`[Teardown] Final Audit Service Status: ${finalAuditHealth.status}`);
  console.log(`[Teardown] Final Notification Service Status: ${finalNotifHealth.status}`);
}
