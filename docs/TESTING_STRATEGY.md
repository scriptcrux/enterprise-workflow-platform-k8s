# Money Forward Workflow Platform — Test Strategy & Testing Pyramid

> **Companion to**: [`DEVELOPMENT.md`](./DEVELOPMENT.md) — Architecture Overview
>
> **Based on**: Senior Quality Engineer, Workflow Platform JD — Money Forward, Nagoya
>
> This document defines the **complete testing strategy** for the Workflow Platform using a **Node.js / TypeScript** toolchain. It maps every layer of the testing pyramid to the platform's architecture, explains _why_ each layer exists, what tools to use, and how to implement them.

---

## Table of Contents

1. [The Testing Philosophy](#1-the-testing-philosophy)
2. [The Testing Pyramid (Platform Edition)](#2-the-testing-pyramid-platform-edition)
3. [Layer 0 — Static Analysis & Linting](#3-layer-0--static-analysis--linting)
4. [Layer 1 — Unit Tests](#4-layer-1--unit-tests)
5. [Layer 2 — API Contract Tests](#5-layer-2--api-contract-tests)
6. [Layer 3 — Kafka / Event Contract Tests](#6-layer-3--kafka--event-contract-tests)
7. [Layer 3c — Kafka Schema Compatibility Testing](#7-layer-3c--kafka-schema-compatibility-testing)
8. [Layer 4 — Service Integration Tests](#8-layer-4--service-integration-tests)
9. [Layer 5 — End-to-End Tests (E2E)](#9-layer-5--end-to-end-tests-e2e)
10. [Layer 6 — Performance & Resilience Tests](#10-layer-6--performance--resilience-tests)
11. [Layer 7 — Security & Tenant Isolation Tests](#11-layer-7--security--tenant-isolation-tests)
12. [CI/CD Integration](#12-cicd-integration)
13. [Observability & Telemetry in Tests](#13-observability--telemetry-in-tests)
14. [Test Data Strategy](#14-test-data-strategy)
15. [Tool Selection Summary](#15-tool-selection-summary)
16. [Risk-Based Testing Matrix](#16-risk-based-testing-matrix)

---

## <a id="1-the-testing-philosophy"></a>1. The Testing Philosophy

The JD explicitly states:

> _"Define and evolve a **risk-based quality strategy** for the platform, covering service contracts, cross-service workflows, **tenant isolation**, failure recovery, performance, and reliability."_

This means we don't test everything equally. We invest testing effort where **the risk is highest** and where **no single component team can validate alone**:

- Contract boundaries between the platform and its consumers (product teams)
- Cross-service workflows that span multiple services and queues
- Tenant isolation guarantees (data, performance, and access)
- Failure modes, retry behaviour, and partial failure recovery
- Performance characteristics under concurrent tenant load

**Key Principles:**

| Principle | What it means in practice |
|---|---|
| **Shift Left** | Catch bugs at the cheapest layer possible (unit > contract > integration > E2E) |
| **Risk-Based** | More tests where failure has the highest impact (tenant isolation, event ordering) |
| **Contract-First** | Define API and event schemas before integration — test the contract, not just the behaviour |
| **Test at the Boundary** | The most valuable tests live at service integration points, not just inside a single service |
| **Deterministic** | Tests must produce the same result every run — no flakiness allowed in CI |

---

## <a id="2-the-testing-pyramid-platform-edition"></a>2. The Testing Pyramid (Platform Edition)

```
                        /\
                       /  \
                      / 7  \         SECURITY & TENANT ISOLATION
                     /------\        (Fewest, but critical)
                    /   6    \
                   /----------\      PERFORMANCE & RESILIENCE
                  /     5      \
                 /--------------\    END-TO-END (E2E) — Playwright
                /       4        \
               /------------------\  SERVICE INTEGRATION TESTS
              /         3b         \
             /----------------------\ KAFKA EVENT CONTRACT TESTS
            /           3a           \
           /----------------------------\ API CONTRACT TESTS (Pact)
          /             2                \
         /----------------------------------\ UNIT TESTS (Vitest)
        /               1                    \
       /--------------------------------------\ STATIC ANALYSIS (ESLint/TSC)
      /                  0                      \
     /--------------------------------------------\

     Faster, Cheaper, More                    Slower, More
     ←── Isolated                              Integrated ──►
```

| Layer | Type | Tool | Speed | Count |
|---|---|---|---|---|
| 0 | Static Analysis / Linting | ESLint, TypeScript Compiler | Seconds | Unlimited |
| 1 | Unit Tests | Vitest | Fast | Many (hundreds) |
| 2 | API Contract Tests | Pact (Consumer-Driven) | Fast-Medium | Moderate |
| 3a | Kafka Event Contract Tests | Pact + Kafkajs | Medium | Moderate |
| 3b | Kafka Schema Compatibility | @kafkajs/confluent-schema-registry + Ajv | Medium | Every schema change |
| 3c | Service Integration Tests | Supertest + Testcontainers | Medium | Selective |
| 4 | End-to-End Tests | Playwright | Slow | Few (critical paths only) |
| 5 | Performance & Resilience | k6 | Long-running | Per release cycle |
| 6 | Security & Tenant Isolation | Playwright + Custom | Per-release | Critical |

---

## <a id="3-layer-0--static-analysis--linting"></a>3. Layer 0 — Static Analysis & Linting

### Why?
The cheapest possible bug prevention — runs before any test. For a TypeScript-first test framework, this catches type errors, import problems, and code style issues instantly.

### Tools

| Tool | Purpose |
|---|---|
| **TypeScript (`tsc`)** | Type-checking all test code — catches wrong payload shapes, missing fields, type mismatches |
| **ESLint + `@typescript-eslint`** | Enforces code style, detects common async/await pitfalls in test code |
| **Prettier** | Consistent formatting — reduces noise in code reviews |

### Setup

```bash
npm install -D typescript @typescript-eslint/parser @typescript-eslint/eslint-plugin eslint prettier
```

**`tsconfig.json`** (for test code):
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "outDir": "./dist"
  },
  "include": ["src/**/*", "tests/**/*"]
}
```

**`.eslintrc.json`**:
```json
{
  "parser": "@typescript-eslint/parser",
  "plugins": ["@typescript-eslint"],
  "extends": [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended-type-checked"
  ],
  "rules": {
    "@typescript-eslint/no-floating-promises": "error",
    "@typescript-eslint/await-thenable": "error",
    "no-console": "warn"
  }
}
```

### In CI
```yaml
# GitHub Actions step
- name: Static Analysis
  run: |
    npx tsc --noEmit
    npx eslint 'tests/**/*.ts'
```

---

## <a id="4-layer-1--unit-tests"></a>4. Layer 1 — Unit Tests

### Why?
Unit tests validate the **business logic of test utilities and shared test helpers** — not the application itself (which is Kotlin). On the Node.js test tooling side, unit tests validate:
- Custom Pact matchers
- Test data factory functions
- Kafka message serialisers/deserialisers used in tests
- Utility functions that build API request payloads

> **Note**: The Kotlin backend has its own unit tests. From the Node.js QA toolchain perspective, "unit tests" here means testing the **test infrastructure itself** to ensure it's reliable.

### Tools

| Tool | Purpose |
|---|---|
| **Vitest** | Fast, native ESM, TypeScript-first test runner. Preferred over Jest for TS projects. |
| **`@faker-js/faker`** | Generates realistic test data (names, emails, IDs, amounts) |

### Setup

```bash
npm install -D vitest @faker-js/faker
```

**`vitest.config.ts`**:
```typescript
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/**/*.ts'],
      thresholds: { lines: 80, functions: 80, branches: 70 }
    }
  }
});
```

**Example — Test Data Factory:**
```typescript
// src/factories/workflow.factory.ts
import { faker } from '@faker-js/faker';

export interface WorkflowPayload {
  type: 'expense-approval' | 'document-submission';
  submittedBy: string;
  tenantId: string;
  steps: Array<{ approver: string; order: number }>;
}

export function buildWorkflow(overrides?: Partial<WorkflowPayload>): WorkflowPayload {
  return {
    type: 'expense-approval',
    submittedBy: faker.string.uuid(),
    tenantId: faker.string.alphanumeric(8),
    steps: [
      { approver: faker.string.uuid(), order: 1 },
      { approver: faker.string.uuid(), order: 2 }
    ],
    ...overrides
  };
}

// tests/unit/workflow.factory.test.ts
import { describe, it, expect } from 'vitest';
import { buildWorkflow } from '../../src/factories/workflow.factory';

describe('buildWorkflow factory', () => {
  it('generates a valid workflow with defaults', () => {
    const w = buildWorkflow();
    expect(w.type).toBe('expense-approval');
    expect(w.steps).toHaveLength(2);
    expect(w.steps[0]!.order).toBeLessThan(w.steps[1]!.order);
  });

  it('allows overriding tenantId', () => {
    const w = buildWorkflow({ tenantId: 'acme-corp' });
    expect(w.tenantId).toBe('acme-corp');
  });
});
```

---

## <a id="5-layer-2--api-contract-tests"></a>5. Layer 2 — API Contract Tests

### Why?
API contract tests answer one fundamental question: **"If the platform API changes, which consumers will break before we deploy to production?"**

This is **Consumer-Driven Contract Testing (CDCT)**:
1. **The Consumer (`workflow-host-app`)** writes tests defining the HTTP interactions it needs (status code, headers, JSON body shape).
2. **Pact captures these expectations** into a standardized contract file (`pacts/workflow-host-app-workflow-api.json`).
3. **The Provider (`workflow-api`)** replays this contract in its own pipeline against its real running Fastify controllers with database access mocked out.

---

### 🧠 Core Concept Revision & Interview Cheatsheet

#### 1. Contract Testing vs. Integration Testing — What's the Difference?
* **Integration Tests**: Spin up containers (PostgreSQL, Kafka) and verify that `Service -> Database` queries work and data is correctly written.
* **Contract Tests**: Intercept the **boundary between two services**. They verify that the consumer and provider agree on:
  * HTTP route paths (`/api/v1/workflows`) and methods (`POST`, `GET`).
  * Custom headers (`x-tenant-id`, `x-user-id`, `content-type`).
  * HTTP status codes (`200`, `201`, `400`, `404`).
  * Response envelope structure (`{ success: true, data: { ... } }` vs `{ success: false, error: { ... } }`).
  * Serialization formats (e.g. ISO 8601 strings for timestamps, numeric types for amounts).

#### 2. Why is the Database Mocked during Provider Pact Verification?
A common misconception is: *"If the database is mocked, how does Pact catch database column changes?"*
* **The Separation of Concerns**:
  * If a developer renames a DB column from `title` to `display_name`, their **Integration/Unit Tests** will fail on the database query.
  * If the developer updates the database query but forgets that the public API must still output `title`, the **Pact Provider Test** fails immediately because the returned JSON is missing `title`.
* This decouples provider testing from live database state, making contract verification lightning fast (~500ms) and 100% deterministic.

#### 3. Matchers vs. Exact Values (`MatchersV3`)
In Pact, consumers should **never assert static IDs or exact timestamps** unless strictly necessary. Instead, use flexible type matchers:
* `uuid('a1b2c3d4-...')`: Ensures the field is a valid RFC 4122 UUID string.
* `integer(3500)`: Ensures the value is an integer type (or numeric).
* `string('EXPENSE')`: Ensures the field is a string, using the example for mock responses.
* `like({ ... })`: Validates the structure and types of object properties without locking in fixed values.

#### 4. Provider States (`given(...)` and `stateHandlers`)
To test different scenarios against the same endpoint (e.g. `200 OK` vs `404 Not Found` for `GET /api/v1/workflows/:id`), the consumer specifies a **Provider State**:
* **Consumer**: `.given('workflow does not exist')`
* **Provider**: Registers `stateHandlers: { 'workflow does not exist': async () => { currentWorkflow = null; } }`
* When Pact executes, it invokes the state handler before firing the HTTP request, allowing the provider mock to return `null` and trigger Fastify's `404 WORKFLOW_NOT_FOUND` error handler.

---

### Implementation & File References

* **Consumer Contract Suite**: [`workflow-api.consumer.pact.test.ts`](../tests/contract/pact/workflow-api.consumer.pact.test.ts)
* **Provider Verification Suite**: [`workflow-api.provider.pact.test.ts`](../tests/contract/pact/workflow-api.provider.pact.test.ts)

```typescript
// Consumer Interaction Example (Zero-State Empty List Contract)
await provider
  .given('no workflows exist')
  .uponReceiving('a request to list workflows when none exist')
  .withRequest({
    method: 'GET',
    path: '/api/v1/workflows',
    headers: { 'x-tenant-id': 'tenant-corp-a', 'x-user-id': 'user-alice' }
  })
  .willRespondWith({
    status: 200,
    headers: { 'content-type': 'application/json; charset=utf-8' },
    body: {
      success: true,
      data: {
        workflows: [],
        total: integer(0),
        limit: integer(50),
        offset: integer(0)
      }
    }
  })
  .executeTest(async (mockServer) => {
    const res = await fetch(`${mockServer.url}/api/v1/workflows`, { headers: { 'x-tenant-id': 'tenant-corp-a', 'x-user-id': 'user-alice' } });
    const body = await res.json();
    expect(body.data.workflows).toEqual([]);
  });
```

---

## <a id="6-layer-3--kafka--event-contract-tests"></a>6. Layer 3 — Kafka / Event Contract Tests

### Why?
In an event-driven architecture, services communicate asynchronously via Kafka topics. If the producer (`workflow-api`) changes the payload shape of `workflow.submitted.v1`, downstream consumers (`notification-service`, `audit-service`) will fail to parse messages, creating **poison pills** in consumer partitions.

### 🧠 Core Concept Revision & Mechanics

#### 1. How MessagePact Works Without a Live Kafka Broker
* Unlike HTTP Pact (which spins up a local mock HTTP server), **Pact Message testing** is transport-agnostic.
* It operates directly on serialized message payloads (JSON / CloudEvents).
* **Consumer Side**: Defines the message structure it expects and verifies that its internal message handler (e.g. `NotificationConsumer.processMessage`) can deserialize and process it without throwing errors.
* **Producer Side**: Passes its event creation function (e.g. `buildWorkflowEvent`) to Pact's `Verifier` using `messageProviders`. Pact compares the generated object to the contract JSON file.

#### 2. CloudEvents 1.0 Specification Invariants
All workflow platform events adhere strictly to the CNCF CloudEvents 1.0 specification:
* `specversion`: Must be `'1.0'`.
* `type`: Unique reverse-DNS event type string (e.g. `workflow.submitted.v1`, `workflow.approved.v1`).
* `source`: Identifying producer service (`workflow-api`).
* `id`: Unique event UUID.
* `time`: ISO 8601 UTC timestamp (`2026-09-21T08:00:00.000Z`).
* `datacontenttype`: Content type (`application/json`).
* `data`: Domain payload containing `workflowId`, `tenantId`, `status`, `actorId`, and custom metadata.

### Implementation & File References

* **Kafka Consumer Contract Suite**: [`workflow-events.consumer.pact.test.ts`](../tests/contract/pact/workflow-events.consumer.pact.test.ts)
* **Kafka Producer Verification Suite**: [`workflow-events.provider.pact.test.ts`](../tests/contract/pact/workflow-events.provider.pact.test.ts)
* **Cross-Service Event Compatibility Test**: [`cross-service-event.contract.test.ts`](../tests/contract/cross-service-event.contract.test.ts)
* **Partition Ordering & Murmur2 Test**: [`kafka-partition-ordering.contract.test.ts`](../tests/contract/kafka-partition-ordering.contract.test.ts)

### Idempotency Test — Duplicate Event Handling

```typescript
// tests/integration/kafka/idempotency.test.ts
import { Kafka } from 'kafkajs';
import { KafkaContainer } from '@testcontainers/kafka';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

describe('Kafka Event Idempotency', () => {
  let kafka: Kafka;

  beforeAll(async () => {
    const container = await new KafkaContainer().start();
    kafka = new Kafka({ brokers: [container.getBootstrapServers()] });
  });

  it('processing the same workflow.approved event twice does not duplicate notifications', async () => {
    const consumer = kafka.consumer({ groupId: 'notification-service-test' });
    await consumer.connect();
    await consumer.subscribe({ topic: 'workflow.approved' });

    const processedIds = new Set<string>();
    let processingCount = 0;

    await consumer.run({
      eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value!.toString()) as { workflowId: string };
        if (!processedIds.has(event.workflowId)) {
          processedIds.add(event.workflowId);
          processingCount++;
        }
        // If workflowId already processed — silently skip (idempotent)
      }
    });

    // Send the same event twice (simulates at-least-once delivery)
    const producer = kafka.producer();
    await producer.connect();
    const duplicate = JSON.stringify({ workflowId: 'wf-test-123', eventType: 'workflow.approved' });
    await producer.send({ topic: 'workflow.approved', messages: [{ value: duplicate }, { value: duplicate }] });

    await new Promise(r => setTimeout(r, 1000)); // allow processing

    expect(processingCount).toBe(1); // Only processed once
  });
});
```

---

## <a id="7-layer-3c--kafka-schema-compatibility-testing"></a>7. Layer 3c — Kafka Schema Compatibility Testing

### Why?
Kafka uses an **at-least-once delivery** model with no built-in schema enforcement — any producer can publish anything to a topic. When the Workflow Platform team changes an event payload (adds a field, renames one, changes a type), **consumers silently break** unless schema compatibility is enforced at the infrastructure level.

The JD calls this out explicitly:

> _"Distributed and event-driven systems. Experience testing... **schema compatibility**, delivery semantics, ordering, retries, and idempotency."_

Schema compatibility testing answers: **"If I change this event's shape, does it break any existing consumer, and in which direction?"**

### Compatibility Modes Explained

```
Schema v1: { workflowId, tenantId, status }
Schema v2: { workflowId, tenantId, status, approvedAt }   <-- added field
Schema v3: { workflowId, tenantId, outcome }               <-- renamed status -> outcome

BACKWARD compatible:  New consumer can read old messages   (v2 reads v1 messages OK)
FORWARD  compatible:  Old consumer can read new messages   (v1 consumer reads v2 messages OK)
FULL     compatible:  Both directions safe                  (v2 <--> v1 fully interoperable)
BREAKING:            Neither direction safe                (v3 vs v1 — renamed field BREAKS consumers)
```

**Rule of thumb for this platform:**  
- New optional fields = `BACKWARD` compatible ✅  
- Removed / renamed fields = `BREAKING` ❌ — requires a versioned migration strategy  
- Changed field types = `BREAKING` ❌  

### Tools

| Tool | Purpose |
|---|---|
| **`@kafkajs/confluent-schema-registry`** | Node.js client for Confluent Schema Registry — registers, fetches, and validates Avro/JSON Schema |
| **`Ajv` (Another JSON Validator)** | Fast JSON Schema validation in TypeScript — validates event payloads against a declared schema |
| **Confluent Schema Registry** | Centrally stores all event schemas and enforces compatibility rules on registration |
| **`avsc`** | TypeScript-friendly Avro schema library for serialisation/deserialisation testing |

### Setup

```bash
npm install -D @kafkajs/confluent-schema-registry ajv ajv-formats avsc
```

---

### Approach A — JSON Schema Validation (Lightweight, No Registry)

Best when you don't yet have a Schema Registry deployed. Validate every event message shape in tests using a shared JSON Schema definition.

```typescript
// src/schemas/workflow-approved.schema.ts
export const workflowApprovedSchema = {
  $schema: 'http://json-schema.org/draft-07/schema#',
  $id: 'workflow.approved',
  type: 'object',
  required: ['eventType', 'workflowId', 'tenantId', 'approvedBy', 'approvedAt'],
  additionalProperties: false,           // strict: no unexpected fields
  properties: {
    eventType:  { type: 'string', const: 'workflow.approved' },
    workflowId: { type: 'string', format: 'uuid' },
    tenantId:   { type: 'string', minLength: 1 },
    approvedBy: { type: 'string', format: 'uuid' },
    approvedAt: { type: 'string', format: 'date-time' },
    metadata: {
      type: 'object',
      required: ['workflowType'],
      properties: {
        workflowType: { type: 'string' },
        currentStep:  { type: 'integer', minimum: 1 },
        totalSteps:   { type: 'integer', minimum: 1 }
      }
    }
  }
};
```

```typescript
// tests/schema/workflow-approved.schema.test.ts
import { describe, it, expect } from 'vitest';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { workflowApprovedSchema } from '../../src/schemas/workflow-approved.schema';

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validate = ajv.compile(workflowApprovedSchema);

describe('workflow.approved — JSON Schema Compliance', () => {
  it('accepts a valid event payload', () => {
    const validEvent = {
      eventType: 'workflow.approved',
      workflowId: '550e8400-e29b-41d4-a716-446655440000',
      tenantId: 'acme-corp',
      approvedBy: '660e8400-e29b-41d4-a716-446655440001',
      approvedAt: '2026-09-17T10:00:00Z',
      metadata: { workflowType: 'expense-approval', currentStep: 2, totalSteps: 2 }
    };
    expect(validate(validEvent)).toBe(true);
  });

  it('rejects event missing required field approvedAt', () => {
    const invalid = {
      eventType: 'workflow.approved',
      workflowId: '550e8400-e29b-41d4-a716-446655440000',
      tenantId: 'acme-corp',
      approvedBy: '660e8400-e29b-41d4-a716-446655440001'
      // approvedAt deliberately missing
    };
    expect(validate(invalid)).toBe(false);
    expect(validate.errors).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ message: "must have required property 'approvedAt'" })
      ])
    );
  });

  it('rejects event with unexpected extra field (strict mode)', () => {
    const withExtra = {
      eventType: 'workflow.approved',
      workflowId: '550e8400-e29b-41d4-a716-446655440000',
      tenantId: 'acme-corp',
      approvedBy: '660e8400-e29b-41d4-a716-446655440001',
      approvedAt: '2026-09-17T10:00:00Z',
      SURPRISE_FIELD: 'this should not be here'  // additionalProperties: false
    };
    expect(validate(withExtra)).toBe(false);
  });
});
```

---

### Approach B — Confluent Schema Registry (Production-Grade)

Best when the platform is scaling and multiple teams are producing/consuming events. The Schema Registry acts as a **schema gate** — a producer cannot publish a message with a breaking schema change without explicit approval.

```typescript
// tests/schema/registry.schema.test.ts
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { describe, it, expect, beforeAll } from 'vitest';

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081'
});

const SUBJECT = 'workflow.approved-value';

describe('Confluent Schema Registry — workflow.approved', () => {
  let currentSchemaId: number;

  beforeAll(async () => {
    // Register the current schema — fails if it's a BREAKING change
    const { id } = await registry.register(
      {
        type: SchemaType.JSON,
        schema: JSON.stringify({
          type: 'object',
          required: ['eventType', 'workflowId', 'tenantId', 'approvedBy', 'approvedAt'],
          properties: {
            eventType:  { type: 'string' },
            workflowId: { type: 'string' },
            tenantId:   { type: 'string' },
            approvedBy: { type: 'string' },
            approvedAt: { type: 'string' }
          }
        })
      },
      { subject: SUBJECT }
    );
    currentSchemaId = id;
  });

  it('can encode and decode a workflow.approved event round-trip', async () => {
    const event = {
      eventType: 'workflow.approved',
      workflowId: '550e8400-e29b-41d4-a716-446655440000',
      tenantId: 'acme-corp',
      approvedBy: '660e8400-e29b-41d4-a716-446655440001',
      approvedAt: '2026-09-17T10:00:00Z'
    };

    const encoded = await registry.encode(currentSchemaId, event);
    const decoded = await registry.decode(encoded);

    expect(decoded).toEqual(event);
  });

  it('BACKWARD compatibility — old consumers can read new schema (new optional field)', async () => {
    // v2 adds an optional field — should be BACKWARD compatible
    const v2Schema = {
      type: 'object',
      required: ['eventType', 'workflowId', 'tenantId', 'approvedBy', 'approvedAt'],
      properties: {
        eventType:     { type: 'string' },
        workflowId:    { type: 'string' },
        tenantId:      { type: 'string' },
        approvedBy:    { type: 'string' },
        approvedAt:    { type: 'string' },
        correlationId: { type: 'string' }  // new OPTIONAL field — backward compatible
      }
    };

    // This should NOT throw — registry allows backward-compatible changes
    await expect(
      registry.register(
        { type: SchemaType.JSON, schema: JSON.stringify(v2Schema) },
        { subject: SUBJECT }
      )
    ).resolves.toBeDefined();
  });

  it('BREAKING — renaming a required field is rejected by the registry', async () => {
    const breakingSchema = {
      type: 'object',
      required: ['eventType', 'workflowId', 'tenantId', 'approvedBy', 'completedAt'],
      properties: {
        eventType:   { type: 'string' },
        workflowId:  { type: 'string' },
        tenantId:    { type: 'string' },
        approvedBy:  { type: 'string' },
        completedAt: { type: 'string' }   // renamed from approvedAt -> BREAKS consumers!
      }
    };

    // The registry MUST reject this if compatibility mode is BACKWARD or FULL
    await expect(
      registry.register(
        { type: SchemaType.JSON, schema: JSON.stringify(breakingSchema) },
        { subject: SUBJECT }
      )
    ).rejects.toThrow(); // Schema Registry returns 409 Conflict for breaking changes
  });
});
```

---

### Schema Evolution Test Matrix

Run this suite on every PR that touches an event schema:

```typescript
// tests/schema/schema-evolution.test.ts
import { describe, it, expect } from 'vitest';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { workflowApprovedSchema } from '../../src/schemas/workflow-approved.schema';

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);

describe('Schema Evolution Safety Matrix', () => {
  const v1Validator = ajv.compile(workflowApprovedSchema);

  describe('BACKWARD compatible changes (safe to ship)', () => {
    it('adding a new OPTIONAL field does not break old consumers', () => {
      // Old message (v1 shape) must still validate against v1 schema
      const oldMessage = {
        eventType: 'workflow.approved',
        workflowId: '550e8400-e29b-41d4-a716-446655440000',
        tenantId: 'acme-corp',
        approvedBy: '660e8400-e29b-41d4-a716-446655440001',
        approvedAt: '2026-09-17T10:00:00Z'
      };
      expect(v1Validator(oldMessage)).toBe(true);
    });
  });

  describe('BREAKING changes (must be caught in CI)', () => {
    it('removing a required field breaks validation', () => {
      const broken = {
        eventType: 'workflow.approved',
        workflowId: '550e8400-e29b-41d4-a716-446655440000'
        // tenantId, approvedBy, approvedAt all missing
      };
      expect(v1Validator(broken)).toBe(false);
    });

    it('changing a field type from string to number breaks consumers', () => {
      const typeMismatch = {
        eventType: 'workflow.approved',
        workflowId: '550e8400-e29b-41d4-a716-446655440000',
        tenantId: 42,           // was string, now number — BREAKING
        approvedBy: '660e8400-e29b-41d4-a716-446655440001',
        approvedAt: '2026-09-17T10:00:00Z'
      };
      expect(v1Validator(typeMismatch)).toBe(false);
    });

    it('changing enum values breaks consumers relying on specific values', () => {
      const wrongEnum = {
        eventType: 'workflow.ACCEPTED',   // was 'workflow.approved'
        workflowId: '550e8400-e29b-41d4-a716-446655440000',
        tenantId: 'acme-corp',
        approvedBy: '660e8400-e29b-41d4-a716-446655440001',
        approvedAt: '2026-09-17T10:00:00Z'
      };
      expect(v1Validator(wrongEnum)).toBe(false);
    });
  });
});
```

---

### Compatibility Gate in CI

This step runs on every PR that modifies a `*.schema.ts` or `*.avsc` file:

```yaml
# .github/workflows/schema-compatibility.yml
name: Schema Compatibility Gate

on:
  pull_request:
    paths:
      - 'src/schemas/**'
      - '**/*.avsc'
      - '**/schema*.ts'

jobs:
  schema-compat:
    runs-on: ubuntu-latest
    services:
      schema-registry:
        image: confluentinc/cp-schema-registry:7.6.0
        ports: ['8081:8081']
        env:
          SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9092
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '22', cache: 'npm' }
      - run: npm ci

      - name: JSON Schema Validation Tests
        run: npx vitest run tests/schema/

      - name: Registry Compatibility Gate
        run: npx vitest run tests/schema/registry.schema.test.ts
        env:
          SCHEMA_REGISTRY_URL: http://localhost:8081

      - name: Fail PR on BREAKING schema change
        if: failure()
        run: |
          echo "::error::BREAKING schema change detected. Consumers will be affected."
          echo "::error::Options: (1) make the change backward-compatible, or (2) version the topic (workflow.approved.v2)"
          exit 1
```

### Schema Versioning Strategy

When a **truly breaking** change is unavoidable:

```
DO NOT:   Modify topic  workflow.approved  in place

DO:       Create new topic  workflow.approved.v2
          Run both topics in parallel during migration window
          Migrate consumers one by one to v2
          Deprecate v1 topic after all consumers have migrated
          Delete v1 topic
```

This dual-publish strategy ensures **zero-downtime schema evolution** across all product teams.

---

## <a id="8-layer-4--service-integration-tests"></a>8. Layer 4 — Service Integration Tests

### Why?
Individual units and contracts may pass but services can still fail when wired together. Integration tests spin up the **real service** (or a close approximation) and test against a real database, real Kafka, and real HTTP endpoints — without a browser or UI.

The JD calls out:

> _"Work with developers to improve the platform's **testability and observability**, including useful telemetry, **controllable test environments**, and reliable test data."_

### Tools

| Tool | Purpose |
|---|---|
| **Testcontainers (`testcontainers`)** | Spin up PostgreSQL, Kafka, Redis in Docker for tests |
| **Supertest** | HTTP integration testing against the live API |
| **`@testcontainers/postgresql`** | Postgres instance for database tests |
| **`@testcontainers/kafka`** | Real Kafka for event-driven integration tests |

### Setup

```bash
npm install -D testcontainers @testcontainers/postgresql @testcontainers/kafka supertest
```

### Example — Workflow Creation Integration Test

```typescript
// tests/integration/api/workflow.integration.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { KafkaContainer } from '@testcontainers/kafka';

const BASE_URL = process.env.API_BASE_URL ?? 'http://localhost:8080';

describe('POST /api/v1/workflows — Integration', () => {
  it('creates a workflow and verifies it appears in GET /api/v1/workflows/:id', async () => {
    const payload = {
      type: 'expense-approval',
      submittedBy: 'user-abc',
      tenantId: 'tenant-x',
      steps: [{ approver: 'mgr-001', order: 1 }]
    };

    // Create
    const create = await request(BASE_URL)
      .post('/api/v1/workflows')
      .set('X-Tenant-ID', 'tenant-x')
      .send(payload)
      .expect(201);

    const { workflowId } = create.body as { workflowId: string };

    // Read back
    const get = await request(BASE_URL)
      .get(`/api/v1/workflows/${workflowId}`)
      .set('X-Tenant-ID', 'tenant-x')
      .expect(200);

    expect((get.body as { status: string }).status).toBe('PENDING');
    expect((get.body as { submittedBy: string }).submittedBy).toBe('user-abc');
  });
});
```

### Testcontainers Live Integration Suite Matrix

The platform includes 5 production-grade live integration test suites that spin up ephemeral PostgreSQL and Kafka Docker containers for deterministic, mock-free validation:

| Test Suite | Containers | Key Failure Modes & Invariants Tested | File Location |
| :--- | :--- | :--- | :--- |
| **Concurrent Idempotency & Atomic Locking** | `PostgreSqlContainer` | • 10-worker atomic race condition on identical `Idempotency-Key`<br>• Exactly-1 mutation guarantee (`RETURNING id`)<br>• Cached replay verification (`x-idempotent-replay: true`)<br>• Multi-tenant key namespace isolation | [`tests/integration/idempotency.live.integration.test.ts`](../tests/integration/idempotency.live.integration.test.ts) |
| **High-Concurrency Connection Pool Stress** | `PostgreSqlContainer` | • 25 concurrent tasks sharing a tiny connection pool (`max: 3`)<br>• Random interleaving across 5 distinct tenants<br>• Forced syntax errors & mid-transaction `ROLLBACK` hygiene<br>• Zero `app.current_tenant_id` session bleed (`is_local = true`) | [`tests/integration/pool-stress.live.integration.test.ts`](../tests/integration/pool-stress.live.integration.test.ts) |
| **Transactional Outbox Relay & Auto-Drain** | `PostgreSqlContainer` + `KafkaContainer` | • Atomic DB dual-write commit (`workflows` + `outbox_events`)<br>• Partition-keyed production to real Kafka broker (`${tenantId}:${workflowId}`)<br>• Broker transport failure recording (`retry_count`, `last_error`)<br>• Chronological backlog auto-drain (`ORDER BY created_at ASC`) | [`tests/integration/outbox-relay.live.integration.test.ts`](../tests/integration/outbox-relay.live.integration.test.ts) |
| **Kafka Dead Letter Queue (DLQ) & Replay** | `KafkaContainer` + `PostgreSqlContainer` | • Ingestion of corrupted raw non-JSON bytes into `workflow.events`<br>• Schema validator failure interception (`SCHEMA_VALIDATION_ERROR`)<br>• Forwarding to `workflow.events.dlq` and writing to `dlq_messages`<br>• Operator REST replay back into `workflow.events` (`status = 'REPLAYED'`) | [`tests/integration/kafka-dlq.live.integration.test.ts`](../tests/integration/kafka-dlq.live.integration.test.ts) |
| **PostgreSQL Row-Level Security (RLS)** | `PostgreSqlContainer` | • PostgreSQL engine-level tenant isolation enforcement<br>• Cross-tenant `SELECT`, `UPDATE`, and `INSERT` blocking<br>• Zero row leakage under non-superuser database roles | [`tests/integration/postgres-rls.live.integration.test.ts`](../tests/integration/postgres-rls.live.integration.test.ts) |

---

## <a id="9-layer-5--end-to-end-tests-e2e"></a>9. Layer 5 — End-to-End Tests (E2E)

### Why?
The JD mentions:

> _"Micro-frontend integration testing. Experience validating **embedded web components** and their interactions with host applications, ideally using **browser-automation tools such as Playwright**."_

E2E tests simulate a real user completing a real workflow — from the UI all the way through to the database. They are the most expensive tests so we limit them to **critical happy paths and critical failure paths only**.

### Tools

| Tool | Purpose |
|---|---|
| **Playwright** | Browser automation — explicit mention in the JD |
| **`@playwright/test`** | Test runner built into Playwright |

### Setup

```bash
npm init playwright@latest
# or
npm install -D @playwright/test
npx playwright install
```

**`playwright.config.ts`**:
```typescript
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 60_000,
  retries: process.env.CI ? 2 : 0,
  reporter: [
    ['html', { outputFolder: 'playwright-report' }],
    ['junit', { outputFile: 'test-results/e2e.xml' }]
  ],
  use: {
    baseURL: process.env.BASE_URL ?? 'http://localhost:3000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure'
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
    { name: 'firefox', use: { ...devices['Desktop Firefox'] } }
  ]
});
```

### Example 1 — Out-of-Office Delegations & Proxy Approvals Flow

```typescript
// tests/e2e/workflow-lifecycle.spec.ts
test('Out-of-Office Delegations & Proxy Approvals: Manager sets delegation in UI and proxy colleague signs off', async ({ page }) => {
  await page.goto('/expenses');
  await page.waitForLoadState('networkidle');

  // Step 1: Bob (Team Lead) configures Out-of-Office Delegation in UI Modal
  await page.getByTestId('tenant-selector').selectOption('tenant-corp-a');
  await page.getByTestId('user-selector').selectOption('user-bob');

  await page.getByTestId('open-delegation-modal-btn').click();
  await expect(page.getByTestId('delegation-modal-content')).toBeVisible();

  // Select Carol White as proxy delegatee and submit
  await page.getByTestId('delegation-delegatee-select').selectOption('user-carol');
  await page.getByTestId('delegation-reason-input').fill('Annual Leave - Proxying to Carol White');
  await page.getByTestId('create-delegation-submit-btn').click();

  // Confirm delegation is active in list and close modal
  await expect(page.getByTestId('delegation-record-card').first()).toBeVisible({ timeout: 5000 });
  await page.getByTestId('close-delegation-modal-btn').click();

  // Step 2: Alice creates and submits an expense under $10,000
  await page.getByTestId('user-selector').selectOption('user-alice');
  const expenseTitle = `Team Offsite Logistics - ${Date.now()}`;
  await page.getByTestId('new-expense-btn').click();
  await page.getByTestId('expense-title-input').fill(expenseTitle);
  await page.getByTestId('expense-amount-input').fill('4200');
  await page.getByTestId('submit-create-btn').click();

  await page.getByText(expenseTitle).click();
  await page.getByTestId('submit-workflow-btn').click();
  await expect(page.getByTestId('workflow-status-badge')).toHaveText(/Pending/i, { timeout: 5000 });

  // Step 3: Switch persona to Carol (Proxy Approver / Delegatee)
  await page.getByTestId('user-selector').selectOption('user-carol');
  await page.getByTestId('nav-approvals-link').click();
  await page.getByText(expenseTitle).click();

  // Step 4: Carol approves on behalf of Bob via active delegation
  const approveBtn = page.getByTestId('approve-workflow-btn');
  await expect(approveBtn).toBeVisible();
  await approveBtn.click();
  await expect(page.getByTestId('workflow-status-badge')).toHaveText(/Approved/i, { timeout: 5000 });

  // Step 5: Verify Compliance Audit Trail
  await page.getByTestId('nav-audit-link').click();
  await expect(page.getByTestId('audit-timeline')).toBeVisible({ timeout: 5000 });
});
```

### Example 2 — Micro-Frontend Widget in Host App

```typescript
// tests/e2e/mfe-integration.spec.ts
test.describe('Workflow Widget Embedded in Host App', () => {
  test('widget renders correctly inside the expense host app without style conflicts', async ({ page }) => {
    await page.goto('/expenses');

    // Ensure the widget web component rendered within Shadow DOM
    const widget = page.locator('[data-testid="workflow-widget-container"]');
    await expect(widget).toBeVisible();

    // Verify custom event bubbling across MFE boundary
    const statusBadge = page.getByTestId('workflow-status-badge');
    await expect(statusBadge).toBeVisible();
  });
});
```

---

## <a id="10-layer-6--performance--resilience-tests"></a>10. Layer 6 — Performance & Resilience Tests

### Why?
The JD states:

> _"Lead **performance and resilience testing** to expose capacity limits, latency risks, **unsafe retry behaviour**, and failure modes before they affect production."_
>
> _"Cloud-native performance analysis. Experience combining load-testing results with **metrics, logs, and traces from Kubernetes-based systems** to isolate application, database, or messaging bottlenecks."_

### Tools

| Tool | Purpose |
|---|---|
| **k6** | Load and performance testing with JavaScript scripting |
| **k6 Cloud / Grafana** | Visualise k6 results with metrics dashboards |
| **Artillery** | Alternative to k6 — YAML-based, good for API load tests |

### Setup

```bash
# k6 is a standalone binary — install separately
choco install k6    # Windows
brew install k6     # macOS
```

### Example — Concurrent Tenant Load Test

```javascript
// tests/performance/concurrent-tenants.k6.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const workflowCreateDuration = new Trend('workflow_create_duration', true);

export const options = {
  scenarios: {
    // Simulates 50 concurrent users from 10 different tenants
    concurrent_tenants: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1m', target: 50 },   // ramp up
        { duration: '5m', target: 50 },   // sustain
        { duration: '1m', target: 0 }     // ramp down
      ]
    }
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],   // 95th percentile under 500ms
    http_req_failed: ['rate<0.01'],     // Error rate under 1%
    errors: ['rate<0.01']
  }
};

const TENANTS = ['tenant-a', 'tenant-b', 'tenant-c', 'tenant-d', 'tenant-e',
                 'tenant-f', 'tenant-g', 'tenant-h', 'tenant-i', 'tenant-j'];

export default function () {
  const tenantId = TENANTS[Math.floor(Math.random() * TENANTS.length)];

  const payload = JSON.stringify({
    type: 'expense-approval',
    submittedBy: `user-${__VU}`,
    tenantId,
    steps: [{ approver: `mgr-${__VU}`, order: 1 }]
  });

  const res = http.post(`${__ENV.BASE_URL}/api/v1/workflows`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-ID': tenantId
    }
  });

  const success = check(res, {
    'status is 201': (r) => r.status === 201,
    'response has workflowId': (r) => JSON.parse(r.body).workflowId !== undefined
  });

  errorRate.add(!success);
  workflowCreateDuration.add(res.timings.duration);

  sleep(1);
}
```

### Example — Kafka Retry Storm Test

```javascript
// tests/performance/kafka-retry-storm.k6.js
// Simulate what happens if the consumer keeps failing and Kafka keeps retrying
export const options = {
  scenarios: {
    retry_storm: {
      executor: 'constant-arrival-rate',
      rate: 1000,           // 1000 events per second
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 100
    }
  },
  thresholds: {
    http_req_duration: ['p(99)<2000']   // Even under storm, 99th percentile < 2s
  }
};
```

---

## <a id="11-layer-7--security--tenant-isolation-tests"></a>11. Layer 7 — Security & Tenant Isolation Tests

### Why?
From the JD:

> _"Contract compatibility, integration behaviour, **tenant isolation**, or coordinated changes across systems."_

Tenant isolation failures are not functional bugs — they are **security vulnerabilities**. These tests explicitly attempt cross-tenant data access and verify it is denied.

### Tools
- **Playwright** (browser-level isolation tests)
- **Supertest** (API-level isolation tests)
- **Custom Node.js scripts** (permission boundary probing)

### Example — Cross-Tenant API Isolation Matrix

```typescript
// tests/security/tenant-isolation.test.ts
import { describe, it, expect } from 'vitest';
import request from 'supertest';

const BASE = process.env.API_BASE_URL!;

const TENANTS = ['tenant-alpha', 'tenant-beta', 'tenant-gamma'];

describe('Cross-Tenant Data Isolation Matrix', () => {
  // For every pair of tenants, verify isolation
  for (const ownerTenant of TENANTS) {
    for (const attackerTenant of TENANTS.filter(t => t !== ownerTenant)) {
      it(`${attackerTenant} cannot read ${ownerTenant} workflows`, async () => {
        // Create under ownerTenant
        const create = await request(BASE)
          .post('/api/v1/workflows')
          .set('X-Tenant-ID', ownerTenant)
          .send({ type: 'expense-approval', submittedBy: 'u1', tenantId: ownerTenant, steps: [] })
          .expect(201);

        const { workflowId } = create.body as { workflowId: string };

        // Access as attackerTenant — must be 404 (not 200, not 403)
        const response = await request(BASE)
          .get(`/api/v1/workflows/${workflowId}`)
          .set('X-Tenant-ID', attackerTenant);

        expect(response.status).toBe(404);
        // 403 Forbidden would confirm the resource exists — 404 is the secure response
      });

      it(`${attackerTenant} cannot list ${ownerTenant} workflows via GET /workflows`, async () => {
        const response = await request(BASE)
          .get('/api/v1/workflows')
          .set('X-Tenant-ID', attackerTenant);

        const workflows = response.body as Array<{ tenantId: string }>;
        const leaked = workflows.filter(w => w.tenantId === ownerTenant);
        expect(leaked).toHaveLength(0);
      });
    }
  }
});
```

### Example — PostgreSQL Row-Level Security (RLS) Engine Validation

```typescript
// tests/security/postgres-rls.test.ts
import { describe, it, expect } from 'vitest';
import { withTenantContext } from '../../services/workflow-api/src/db/client.js';

describe('PostgreSQL Row-Level Security (RLS) Hardware Enforcement', () => {
  it('enforces WITH CHECK constraint to prevent cross-tenant writes at the DB kernel level', () => {
    // Attempting to insert a row for tenant-beta while app.current_tenant_id is tenant-alpha
    // throws: "ERROR: new row violates row-level security policy for table 'workflows'"
  });

  it('guarantees SELECT queries without WHERE tenant_id filter are physically partitioned by RLS', () => {
    // Queries execute with SELECT set_config('app.current_tenant_id', tenantId, true)
    // ensuring zero cross-tenant row leakage across pooled connections
  });
});
```


---

## <a id="12-cicd-integration"></a>12. CI/CD Integration

### Pipeline Stages

```
GitHub Push / PR
      |
      v
+---------------------+
| Stage 1: Fast       |    < 5 minutes
| - tsc --noEmit      |
| - ESLint            |
| - Unit Tests (Vitest)|
+---------------------+
      |
      v
+---------------------+
| Stage 2: Contracts  |    < 10 minutes
| - Pact Consumer     |
| - Kafka MessagePact  |
| - Publish to Broker  |
+---------------------+
      |
      v
+---------------------+
| Stage 3: Integration|    < 15 minutes
| - Testcontainers    |
| - Supertest API     |
| - Tenant Isolation  |
+---------------------+
      |
      v
+---------------------+
| Stage 4: E2E        |    < 20 minutes (on staging env)
| - Playwright        |
| - Critical paths    |
+---------------------+
      |
      v
+---------------------+
| Stage 5: Release    |    On release branch only
| - k6 Performance    |
| - Security Matrix   |
| - Pact Provider     |
|   Verification      |
+---------------------+
```

### GitHub Actions Workflow

```
1. Developer PR Pipeline (.github/workflows/dev-pr.yml) — Trigger: Pull Requests & feature branches
   ├── Stage 1: Fast Static Analysis, Typecheck & Unit/UI Tests (< 1 min)
   └── Stage 2: Contract Schemas, Pact CDCT, Security Matrix & Dev Can-I-Deploy Gate
   🛑 FAILS FAST & BLOCKS PR MERGE IF ANY TEST FAILS

2. QA & Release Pipeline (.github/workflows/ci.yml) — Trigger: Push / Merge to 'main'
   ├── Stage 1 & 2: Static Analysis, Monorepo Typecheck & Unit Tests
   ├── Stage 3 & 4: Contract Schemas, Security Matrix & Live RLS Integration
   ├── Stage 5: Playwright End-to-End User Journeys (Multi-tenant isolation & multi-tier routing)
   └── Stage 6: Pact Can-I-Deploy QA Gate, Contract Publishing & Deployment Recording
```

### GitHub Actions Workflows

#### 1. Developer PR Pipeline (`.github/workflows/dev-pr.yml`)
```yaml
name: "Developer PR & Branch Verification (Fail-Fast)"

on:
  pull_request:
    branches: [main, master, develop]
  push:
    branches: ['feat/**', 'fix/**', 'dev', 'develop']

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  fast-checks:
    name: "1. Typecheck, Unit & Frontend Component Tests"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npm run check:fast

  contracts-and-security:
    name: "2. API, Event, Pact & Security Matrix"
    runs-on: ubuntu-latest
    needs: [fast-checks]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npm run test:contract
      - run: npm run test:pact
      - run: npm run test:security
      - run: npm run pact:can-i-deploy -- --pacticipant workflow-api --version ${{ github.sha }} --to-environment dev
        env:
          PACT_BROKER_URL: ${{ secrets.PACT_BROKER_URL || 'http://localhost:9292' }}
          PACT_BROKER_TOKEN: ${{ secrets.PACT_BROKER_TOKEN }}
```

#### 2. QA & Release Pipeline (`.github/workflows/ci.yml`)
```yaml
name: Workflow Platform Enterprise CI

on:
  push:
    branches: [main, master]

jobs:
  static-and-unit:
    name: "Stage 1 & 2: Static Analysis, Monorepo Typecheck & Unit/UI Tests"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npm run typecheck
      - run: npm run test:unit

  contracts-and-security:
    name: "Stage 3 & 4: Contract Schemas, Security Matrix & Live RLS Integration"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npm run test:contract && npm run test:pact
      - run: npm run test:security && npm run test:integration

  e2e:
    name: "Stage 5: Playwright End-to-End User Journeys"
    runs-on: ubuntu-latest
    needs: [static-and-unit, contracts-and-security]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npx playwright install --with-deps chromium
      - run: npm run test:e2e
        env: { CI: true }

  pact-deploy-gate:
    name: "Stage 6: Pact Can-I-Deploy Matrix & QA Deployment Gate"
    runs-on: ubuntu-latest
    needs: [contracts-and-security, e2e]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22, cache: 'npm' }
      - run: npm ci
      - run: npm run pact:publish
      - run: npm run pact:can-i-deploy -- --pacticipant workflow-api --version ${{ github.sha }} --to-environment qa
      - run: npm run pact:record-deployment -- --pacticipant workflow-api --version ${{ github.sha }} --environment qa
```

---

## <a id="13-observability--telemetry-in-tests"></a>13. Observability & Telemetry in Tests

### Why?
The platform requires end-to-end trace correlation and metrics isolation across services:

> _"Cloud-native performance analysis. Experience combining load-testing results with **metrics, logs, and traces from Kubernetes-based systems** to isolate application, database, or messaging bottlenecks."_

Tests alone are not enough — you need to correlate test failures with what's happening inside the system and explicitly test telemetry instrumentation itself.

### Telemetry Test Suites in Platform

| Test Suite | Purpose | Key Assertions |
|---|---|---|
| `structured-logger.test.ts` | Unit tests for `StructuredLogger` | Verifies ISO 8601 timestamps, log levels (`info`, `warn`, `error`), JSON formatting, `trace_id`, `span_id`, `tenant_id`, and `workflow_id` inclusion. |
| `db-query-spans.test.ts` | Unit tests for Drizzle / PostgreSQL OpenTelemetry spans | Verifies `AsyncLocalStorage` context propagation, child span creation (`db.query`), SQL query attributes, latency timing, and exception handling. |
| `client-tracing.test.ts` | Unit tests for Frontend W3C client | Verifies `generateTraceparent()` (32-hex trace ID, 16-hex span ID, `00-...-01`), `generateCorrelationId()`, `createTracedHeaders()`, and `tracedFetch()`. |
| `trace-waterfall.integration.test.ts` | Integration tests for Distributed Trace Waterfall | Validates end-to-end W3C `traceparent` propagation from Fastify Gateway ➔ Outbox ➔ Kafka ➔ Audit & Notification Consumers with matching root `traceId`. |
| `health-readiness-probes.test.ts` | Unit tests for Deep Health & Readiness Probes | Verifies `/health/live` (process uptime & memory) and `/health/ready` (PostgreSQL `SELECT 1` & Kafka cluster connectivity). |
| `observability-configs.test.ts` | Configuration tests for Prometheus & Grafana | Validates Prometheus production alerting rules syntax and provisioned Grafana dashboard definitions. |

### Correlation ID & W3C Traceparent in Tests

```typescript
// packages/telemetry/src/client.ts
import { generateTraceparent, generateCorrelationId, createTracedHeaders } from '@workflow/telemetry/client';

const { traceparent, traceId } = generateTraceparent();
const correlationId = generateCorrelationId('test-approval-flow');

// Use it in API requests:
await request(BASE_URL)
  .post('/api/v1/workflows')
  .set('traceparent', traceparent)
  .set('x-correlation-id', correlationId)
  .set('x-tenant-id', 'tenant-corp-a')
  .send(payload);

// Query Jaeger API directly in integration tests to assert full trace waterfall
const jaegerRes = await fetch(`http://localhost:16686/api/traces/${traceId}`);
const traceJson = await jaegerRes.json();
expect(traceJson.data[0].spans.length).toBeGreaterThanOrEqual(1);
```

---

## <a id="14-test-data-strategy"></a>14. Test Data Strategy

### Rules

1. **Each test creates its own data** — no shared state between tests
2. **Use factories, not fixtures** — generated data is less brittle than static JSON files
3. **Tenant IDs are test-scoped** — use a unique `tenantId` per test to prevent cross-test contamination
4. **Clean up after** — use `afterEach`/`afterAll` to delete created workflows via the API

```typescript
// src/utils/test-tenant.ts
import { faker } from '@faker-js/faker';

export function createTestTenantId(): string {
  return `test-${faker.string.alphanumeric(8).toLowerCase()}`;
}

// In tests:
describe('Workflow API', () => {
  const tenantId = createTestTenantId(); // Unique per describe block

  afterAll(async () => {
    // Clean up all workflows for this test tenant
    await request(BASE_URL)
      .delete(`/api/v1/tenants/${tenantId}/workflows`)
      .set('X-Admin-Token', process.env.ADMIN_TOKEN!);
  });
});
```

---

## <a id="15-tool-selection-summary"></a>15. Tool Selection Summary

| Category | Tool | Why Chosen |
|---|---|---|
| **Test Runner** | **Vitest** | Native TypeScript support, fast, compatible with ESM, good coverage tooling |
| **E2E Browser Tests** | **Playwright** | Explicitly mentioned in JD; multi-browser, stable, great for embedded components |
| **Accessibility (a11y)** | **@axe-core/playwright** | Automated WCAG 2.1 AA audits across host app, widget, and modal dialogs |
| **API Contract Tests** | **Pact JS** | Industry standard for Consumer-Driven Contract Testing; has broker for multi-team usage |
| **Kafka Event Tests** | **Pact JS (MessagePact) + kafkajs** | Same contract framework extended to async messages |
| **Kafka Partitioning** | **Murmur2 Hashing Algorithm** | Strict partition routing guarantee (`${tenantId}:${workflowId}`) for in-order delivery |
| **Integration HTTP Testing** | **Supertest / Fastify .inject()** | Lightweight, pairs naturally with Vitest, great for API integration testing |
| **Real Services in Tests** | **Testcontainers** | Spins up real Postgres/Kafka/Redis in Docker — no mocking needed |
| **Performance / Load** | **k6** | Multi-scenario load scripts, noisy-neighbor flood testing, Grafana native |
| **Static Analysis** | **TypeScript + ESLint** | Catches type/contract mismatches in test code before runtime |
| **Test Data** | **@faker-js/faker** | Realistic data generation; avoids hardcoded values |
| **CI Pipeline** | **GitHub Actions + CircleCI** | Both mentioned in JD; GHA for PR feedback, CircleCI for heavier test stages |
| **Deployment Verification** | **ArgoCD + Playwright smoke tests** | Post-deploy smoke test triggered by ArgoCD health check |

---

## <a id="16-risk-based-testing-matrix"></a>16. Risk-Based Testing Matrix

Based on the JD responsibilities and platform architecture:

| Risk | Severity | Test Layer | Tool / Test Suite | Frequency |
|---|---|---|---|---|
| Tenant data leakage | 🔴 Critical | Security + Integration | `postgres-rls.test.ts`, `tenant-isolation.test.ts` | Every PR |
| DB Connection Pool Context Poisoning | 🔴 Critical | Security | `postgres-pool-leak.test.ts` | Every PR |
| Kafka schema breaking change | 🔴 Critical | Event Contract | `workflow-events.consumer.pact.test.ts`, `schema-compatibility.test.ts` | Every PR |
| API contract broken for consumer | 🔴 Critical | API Contract | `workflow-api.consumer.pact.test.ts`, `workflow-api.provider.pact.test.ts` | Every PR |
| Event ordering failure / Race conditions | 🔴 Critical | Contract | `kafka-partition-ordering.contract.test.ts` (Murmur2) | Every PR |
| Noisy Neighbor / Platform DDoS | 🟠 High | Performance | `k6-noisy-neighbor.js` (429 Throttling) | Every release |
| Micro-frontend widget crashes host app | 🟠 High | E2E | `mfe-integration.spec.ts` (Playwright Error Boundary) | Every PR |
| Workflow SLA breach & missed escalation | 🟠 High | Unit / State Machine | `workflow-sla-escalation.test.ts` (Fake Timers) | Every PR |
| Broken Distributed Trace Waterfall | 🟠 High | Integration / Observability | `trace-waterfall.integration.test.ts` | Every PR |
| WCAG Accessibility Non-compliance | 🟡 Medium | E2E / a11y | `accessibility.spec.ts` (`@axe-core/playwright`) | Every PR |
| CI pipeline flakiness | 🟡 Medium | All layers | Vitest retries & Playwright tracing | Continuously |
| AI agent actor misbehaviour (future) | 🔵 Low-Future | E2E | Playwright | When AX features ship |

---

*Document prepared by the QA team for the Money Forward Workflow Platform.*

*Companion document: [`DEVELOPMENT.md`](./DEVELOPMENT.md) — Full Architecture Overview*

*Last updated: September 2026*
