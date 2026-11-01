# Performance Testing & Test Coverage Guide

This document details the performance testing suite, execution commands, Service Level Agreements (SLAs), and testing coverage strategy for the multi-tenant workflow platform.

---

## 1. Performance Testing Suite (`tests/perf`)

The platform uses **[k6](https://k6.io/)** for HTTP API performance testing and the **[`xk6-kafka`](https://github.com/mostafa/xk6-kafka)** extension for direct Apache Kafka ingestion and Dead Letter Queue (DLQ) stress testing.

### Test Scenarios & Commands

| Command | Script | Target Service / Layer | Purpose & Architecture Focus |
| :--- | :--- | :--- | :--- |
| `npm run test:perf` | `tests/perf/k6-workflow-load.js` | `workflow-api` (HTTP) | **Full Lifecycle Load**: Create Draft $\to$ Fetch $\to$ Submit $\to$ Step Approve $\to$ Filtered List polling with idempotency keys. |
| `npm run test:perf:noisy-neighbor` | `tests/perf/k6-noisy-neighbor.js` | `workflow-api` (HTTP) | **Tenant Isolation & Rate Limiting**: 200 req/s abusive flooder vs 15 req/s VIP guaranteed tenant with 429 throttle verification. |
| `npm run test:perf:spike` | `tests/perf/k6-spike.js` | `workflow-api` (HTTP) | **Resilience & Surge Recovery**: Rapid spike ($10 \to 150$ VUs) validating error containment and zero 5xx responses. |
| `npm run test:perf:soak` | `tests/perf/k6-soak.js` | `workflow-api` (HTTP) | **Endurance & Leak Detection**: 10-minute sustained load monitoring PostgreSQL connection pool and memory stability. |
| `npm run test:perf:sse` | `tests/perf/k6-sse-notifications.js` | `notification-service` (HTTP) | **Real-Time Streaming**: Concurrent Server-Sent Events (SSE) listeners (`/api/v1/stream`) and notifications query polling. |
| `npm run test:perf:kafka:lag` | `tests/perf/k6-kafka-consumer-lag.js` | `Kafka` $\to$ Consumers | **Consumer Group Scale & Lag**: Direct 500 CloudEvents/s ingestion into `workflow.events` measuring consumer group processing latency. |
| `npm run test:perf:kafka:dlq` | `tests/perf/k6-kafka-dlq-stress.js` | `Kafka` $\to$ DLQ Pipeline | **DLQ Poison Pill Injection**: Emits corrupt non-JSON & schema-violating records directly to Kafka to test consumer isolation & recovery. |

---

## 2. Direct Kafka Testing via `xk6-kafka` Extension

The [`xk6-kafka`](https://github.com/mostafa/xk6-kafka) extension compiles a custom k6 binary capable of producing and consuming partition-keyed CloudEvents 1.0 messages directly to Apache Kafka brokers.

### Building & Running via Docker

To eliminate local Go compiler dependencies, build and run using Docker:

```bash
# 1. Build the custom xk6-kafka runner image
npm run test:perf:kafka:build

# 2. Run high-throughput consumer lag benchmark
npm run test:perf:kafka:lag

# 3. Run DLQ poison pill stress test
npm run test:perf:kafka:dlq
```

---

## 3. Service Level Agreements (SLAs) & Thresholds

| Endpoint / Metric | Threshold (p95) | Threshold (p99) | Error Rate Max |
| :--- | :--- | :--- | :--- |
| `POST /api/v1/workflows` (Create Draft) | $< 150\text{ ms}$ | $< 300\text{ ms}$ | $< 1\%$ |
| `POST /api/v1/workflows/:id/submit` | $< 150\text{ ms}$ | $< 300\text{ ms}$ | $< 1\%$ |
| `POST /api/v1/workflows/:id/approve` | $< 150\text{ ms}$ | $< 300\text{ ms}$ | $< 1\%$ |
| `GET /api/v1/workflows` (Filtered Query) | $< 100\text{ ms}$ | $< 250\text{ ms}$ | $< 1\%$ |
| `GET /api/v1/stream` (SSE Connection) | $< 150\text{ ms}$ | $< 300\text{ ms}$ | $< 1\%$ |
| `GET /api/v1/notifications` | $< 100\text{ ms}$ | $< 200\text{ ms}$ | $< 1\%$ |
| Flooder Throttling Rate (`noisy-neighbor`) | N/A | N/A | $> 70\%$ throttled (HTTP 429) |
| Direct Kafka Production (`kafka_produce_duration`) | $< 50\text{ ms}$ | $< 100\text{ ms}$ | $< 1\%$ |

---

## 4. Environment Variables for Performance Tests

You can customize the base target URLs and Kafka broker endpoints:

```bash
# Custom HTTP endpoints
API_URL=http://localhost:3000 npm run test:perf
NOTIFICATION_URL=http://localhost:3001 npm run test:perf:sse

# Custom Kafka Brokers for xk6-kafka
KAFKA_BROKERS=localhost:9092 npm run test:perf:kafka:lag
```

---

## 5. Test Pyramid & Quality Strategy

The repository employs an 8-layer quality pyramid:
1. **Unit Tests (`tests/unit`)**: Workflow state machine, token bucket rate limiting, SLA escalation timers, structured logging.
2. **Integration Tests (`tests/integration`)**: Live containerized PostgreSQL RLS, outbox relay loops, trace waterfall, and DLQ handling.
3. **Contract Tests (`tests/contract`)**: HTTP Pact consumer/provider tests, Kafka partition ordering, and Schema Registry compatibility.
4. **End-to-End Tests (`tests/e2e`)**: Playwright multi-step approvals, Shadow DOM microfrontend event bus, and WCAG accessibility scans.
5. **Security Tests (`tests/security`)**: Database connection pool cleanup hygiene and multi-tenant boundary enforcement.
6. **Performance Tests (`tests/perf`)**: Full HTTP lifecycle load, noisy neighbor isolation, spike resilience, soak endurance, and direct Kafka stream benchmarks.



## why we need xk6-kafka extension?

By default, k6 only speaks HTTP, WebSockets, and gRPC. It has absolutely no built-in capability to communicate natively with Apache Kafka brokers. [1, 2, 3] 
If your System Under Test (SUT) relies heavily on an asynchronous, event-driven architecture (like the Money Forward project you shared earlier, which specifically highlights Kafka-based events), you cannot test it using standard http.post() requests.
This is exactly why we need the [xk6-kafka extension](https://github.com/grafana/xk6-kafka). It extends the k6 engine, allowing your load test scripts to act as native Kafka Producers and Consumers. [2, 4] 

------------------------------
## The 4 Core Reasons We Need It

## 1. Bypassing the API Gateway (Testing True Event-Driven Scale)

In a traditional setup, k6 hits a REST API endpoint, which then publishes a message to Kafka.
If you want to test the scale of your backend event consumers without being limited by your API Gateway's bandwidth, you use xk6-kafka to flood Kafka topics directly. This lets you measure exactly how fast your system processes async data under pressure. [4] 
## 2. Native Binary Protocol Communication
Kafka doesn't understand raw HTTP text. It operates over its own optimized binary TCP protocol. The xk6-kafka extension bundles a Go-based Kafka client right into the k6 executable, enabling it to open native connections to Kafka brokers (like AWS MSK, Confluent Cloud, or self-hosted Kafka clusters). [4] 
## 3. Enterprise Serialization Formats (Avro & JSON Schema)
In real-world platforms, microservices don't just send plain text to Kafka. They use strictly enforced formats to save space and guarantee data structure. The extension natively supports these: [4] 


* Apache Avro
* JSON Schema
* Schema Registry Integration (automatically fetching schemas over the wire) [4] 


## 4. Validating Complex Event Workflows (Publish & Consume)
With xk6-kafka, you can create tests that validate the entire lifecycle of an event: [4] 

   1. Produce: Send a "Job Created" event into topic-A.
   2. Listen (Consume): Block and wait on topic-B to verify that your processing engine successfully handled the task and outputted the correct result. [4, 5] 

------------------------------
## What Changes in the Code?
To use it, you build a custom k6 binary using the xk6 compiler. In your JavaScript code, you get access to a brand-new module: k6/x/kafka. [6, 7] 
Here is what a script looks like using the extension:

```js
import { check } from 'k6';// ⬇️ IMPORTING THE KAFKA EXTENSION EXTENSIONimport { Writer, SchemaRegistry } from 'k6/x/kafka'; 
// 1. Initialize the Kafka client configurationconst writer = new Writer({
  brokers: ['kafka-broker.internal.svc:9092'],
  topic: 'workflow-events-topic',
});
export const options = {
  vus: 50,
  duration: '1m',
};
export default function () {
  // 2. Format a message payload
  const message = {
    key: 'tenant-123',
    value: JSON.stringify({ event: 'USER_CHECKOUT', amount: 150.00 })
  };

  // 3. Publish directly to the Kafka cluster natively!
  const error = writer.produce({ messages: [message] });

  // 4. Assert that Kafka acknowledged the message safely
  check(error, { 'Message delivered successfully': (err) => err === null });
}
```

## Tie-In: How It Connects to the k6 Operator
If you are combining this with the k6 Operator, your platform engineering team will build a custom Docker image that includes the xk6-kafka extension binary inside it.
When your K6 operator manifest runs, the distributed runner pods will execute this exact Kafka script, allowing 10 separate pods to pump millions of events per second straight into your Kafka architecture to benchmark your system's breaking point.





