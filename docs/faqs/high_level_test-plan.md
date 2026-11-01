# Architecture:

For this architecture, contract testing should be implemented across **three distinct integration boundaries**. These layers isolate dependencies so client products, workflow engines, and downstream consumers can deploy independently without breaking changes.

---

### 1. Consumer-Driven HTTP/REST Contracts (Host Apps $\leftrightarrow$ Workflow Gateway)



This is the ingress layer where host products (Expense, Invoice, HR) initiate workflows, submit approvals, or query status via the API Gateway.

* **Tooling:** **Pact** (HTTP consumer-driven contracts) or OpenAPI Bi-Directional Contract Testing (PactFlow).
* **What to Test:**
* **Payload Schema & Field Compatibility:** Ensure required fields (`workflow_id`, `tenant_id`, `action_type`, payload definitions) match expectations between both sides.


* **Status Transitions & Responses:** Verify that responses for different workflow states (`DRAFT`, `SUBMITTED`, `IN_REVIEW`, `APPROVED`, `REJECTED`) provide the exact schema host clients rely on.


* **Tenant Context & Error Handling:** Contract assertions for standard HTTP error contracts (e.g., `401 Unauthorized`, `403 Forbidden` on tenant mismatch, `409 Conflict` on concurrent approval race conditions).


* **Key Benefit:** Prevents breaking host application integrations when updating the Core Engine’s REST APIs.



---

### 2. Micro-Frontend (MFE) Contracts (Host Application Shell $\leftrightarrow$ Embedded Widgets)



The architecture uses embedded React micro-frontends (approval widgets, submission forms) inside host applications. MFE contract testing verifies the browser-level interface boundary.

* **Tooling:** **TypeScript schema definitions** (via shared npm packages/monorepo packages), **Zod/Joi runtime schema validation**, or custom Pact tests for bridge APIs/CustomEvents.
* **What to Test:**
* **Props & Mount API Contract:** Verify that host apps pass the correct props interface to the widget (e.g., `tenantId`, `workflowInstanceId`, `userId`, `onActionComplete`, `themeConfig`).
* **Event Bus / Window PostMessage Contract:** If host apps and widgets communicate via browser `CustomEvent` or `postMessage`, contract-test the event name, action signatures, and event payload structures.
* **CSS/DOM Isolation Boundary:** Ensure CSS class namespaces, shadow DOM encapsulation, or styling tokens don't leak or collide with host applications.


* **Key Benefit:** Ensures a host app deploy doesn't pass undefined props that crash the embedded workflow widget at runtime.

---

### 3. Asynchronous Event / Message Contracts (Engine $\leftrightarrow$ Kafka $\leftrightarrow$ Downstream Consumers)



The engine emits workflow state events via Kafka to downstream services (Notification Service, Host App State Sync, Audit Logs).

* **Tooling:** **Pact Message** (for asynchronous consumer-driven contracts) or **Confluent Schema Registry** (using Avro or Protobuf with strict compatibility checks).
* **What to Test:**
* **State Event Payloads:** Validate the schema of events published to `WorkflowEvents` (e.g., `SubmittedEvent`, `ApprovedEvent`, `EscalatedEvent`).


* **Metadata Headers Contract:** Ensure critical headers like `tenant_id`, `trace_id`, `idempotency_key`, and `timestamp` are always present and formatted correctly.
* **Backward / Forward Compatibility:** If using Protobuf/Avro, enforce `BACKWARD_TRANSITIVE` or `FULL` compatibility rules in the CI pipeline to ensure older consumers can safely read messages when new attributes are added.


* **Key Benefit:** Guarantees that consumer services (like email notifications or financial audit synchronizers) don't drop events or crash from unexpected Kafka payload changes.



---

### Recommended Implementation Priority

| Priority | Layer | Primary Tool | Why Start Here? |
| --- | --- | --- | --- |
| **Phase 1** | **Kafka Message Contracts**<br> | Schema Registry (Avro/Protobuf) or Pact Message | Asynchronous regressions are the hardest to debug in production and can silently corrupt host data.

 |
| **Phase 2** | **HTTP Ingress Boundary**<br> | Pact (Consumer-Driven Contracts) | Fast feedback loop; prevents breaking client apps calling Core Engine APIs.

 |
| **Phase 3** | **Micro-Frontend Interface**<br> | Shared TS Interfaces + Zod / PostMessage Contracts | Locks down runtime widget stability inside client web shells.

 |



# for phase 1 you mentioned scheme registy or pact message? why is that

The reason for mentioning both **Schema Registry (Avro/Protobuf)** and **Pact Message** is that they solve contract safety at two completely different angles: **Schema Governance (Producer-side)** versus **Consumer-Driven Behavioral Contracts (Consumer-side)**.

---

### 1. Schema Registry (Avro / Protobuf / JSON Schema)

A Schema Registry sits in the Kafka infrastructure to govern the shape of binary messages passing through topics.

* **Approach:** **Producer-Driven.** The producer (Workflow Core Engine) defines the schema (e.g., `WorkflowEvent.avsc`), registers it, and Kafka blocks messages that don't match.
* **Compatibility Enforcement:** It verifies rules like `BACKWARD`, `FORWARD`, or `FULL_TRANSITIVE`. For instance, it prevents you from deleting a required field or altering a data type in a way that breaks binary deserialization.
* **The Limitation:** It only verifies **syntactic structure** (e.g., "Field `status` is a `string`"), not **consumer assumptions** (e.g., "Notification Service assumes `status` will only ever be `APPROVED` or `REJECTED`, but the engine just sent `ESCALATED`"). It also doesn't tell you *which* consumer is using *which* fields.

---

### 2. Pact Message

Pact Message is an asynchronous testing framework that applies consumer-driven contract testing to messaging systems.

* **Approach:** **Consumer-Driven.** Downstream services (e.g., Notification Service, Audit Sync) write tests stating:
> *"When an event titled `WorkflowApproved` arrives, I expect these specific fields (`tenantId`, `workflowId`, `approverEmail`), and my internal event handler must be able to parse and handle it successfully."*


* **Compatibility Enforcement:** The consumer generates a contract JSON file (a "pact"). The producer's CI pipeline runs a verification test to ensure it actually emits messages matching every downstream consumer's expectations.
* **The Limitation:** It is an application-level test executed in CI/CD; it does not intercept or validate wire payloads in real-time on the live broker.

---

### Comparison Summary

| Dimension | Schema Registry (Avro/Protobuf) | Pact Message |
| --- | --- | --- |
| **Model** | Producer-driven (Top-down) | Consumer-driven (Bottom-up) |
| **Verification Point** | Build/deploy time & Broker serialization | Unit/Integration test suites in CI pipelines |
| **Scope** | Wire format, field types, syntax, backward compatibility | Semantic meaning, consumer usage, handler correctness |
| **Field Pruning Insight** | None (you don't know who reads what) | High (reveals if a field is unused by any consumer) |

---

### Which Should You Choose for Phase 1?

* **Choose Schema Registry first if:** You are already using Avro or Protobuf with Kafka, or you need hard runtime guards at the broker level to prevent malformed serialization from landing in topics.
* **Choose Pact Message first if:** Your Kafka events use plain JSON payloads, host product teams are building independently with varying release schedules, and you want to ensure downstream services don't crash when consumer handlers read engine events.

In enterprise platforms like Money Forward, the **gold standard is combining both**: Schema Registry ensures wire-level backward compatibility, while Pact Message ensures each downstream team's business logic remains unbroken before deployment.


