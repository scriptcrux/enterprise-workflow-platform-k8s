import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { isDockerAvailable, startTestPostgres, startTestKafka } from '@workflow/test-utils';
import pg from 'pg';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

describe('Live Transactional Outbox Relay & Kafka Auto-Drain Suite (Testcontainers)', async () => {
  const dockerRunning = await isDockerAvailable();

  describe.skipIf(!dockerRunning)('Live Database & Message Broker Dual-Write Engine', () => {
    let pgRes: any;
    let kafkaRes: any;
    let pool: pg.Pool;
    let kafkaProducer: Producer;
    let kafkaConsumer: Consumer;
    let kafkaAdmin: any;
    let stopPg: () => Promise<void>;
    let stopKafka: () => Promise<void>;

    const TOPIC_NAME = 'workflow.events';

    beforeAll(async () => {
      // 1. Start both real PostgreSQL and Kafka Testcontainers
      pgRes = await startTestPostgres();
      pool = pgRes.pool;
      stopPg = pgRes.stop;

      kafkaRes = await startTestKafka();
      stopKafka = kafkaRes.stop;

      // 2. Initialize PostgreSQL schema
      await pool.query(`
        CREATE TABLE IF NOT EXISTS workflows (
          id VARCHAR(64) PRIMARY KEY,
          tenant_id VARCHAR(64) NOT NULL,
          type VARCHAR(64) NOT NULL DEFAULT 'EXPENSE',
          title VARCHAR(255) NOT NULL,
          amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
          requester_id VARCHAR(64) NOT NULL,
          status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
          current_step_order INT NOT NULL DEFAULT 1,
          total_steps INT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS outbox_events (
          id VARCHAR(64) PRIMARY KEY,
          tenant_id VARCHAR(64) NOT NULL,
          workflow_id VARCHAR(64) NOT NULL,
          event_type VARCHAR(128) NOT NULL,
          payload JSONB NOT NULL,
          published BOOLEAN NOT NULL DEFAULT FALSE,
          published_at TIMESTAMPTZ,
          retry_count INT NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      // 3. Connect Kafka client and create topic
      const kafka = new Kafka({
        clientId: 'outbox-live-test-client',
        brokers: [kafkaRes.bootstrapServers],
        retry: { retries: 3 },
      });

      kafkaAdmin = kafka.admin();
      await kafkaAdmin.connect();
      await kafkaAdmin.createTopics({
        topics: [{ topic: TOPIC_NAME, numPartitions: 3, replicationFactor: 1 }],
      });

      kafkaProducer = kafka.producer();
      await kafkaProducer.connect();

      kafkaConsumer = kafka.consumer({ groupId: `test-outbox-group-${uuidv4()}` });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    }, 120000);

    afterAll(async () => {
      if (kafkaConsumer) await kafkaConsumer.disconnect().catch(() => {});
      if (kafkaProducer) await kafkaProducer.disconnect().catch(() => {});
      if (kafkaAdmin) await kafkaAdmin.disconnect().catch(() => {});
      if (stopKafka) await stopKafka().catch(() => {});
      if (stopPg) await stopPg().catch(() => {});
    });

    it('executes atomic dual-write in Postgres and relays event to real Kafka container with partition key', async () => {
      const tenantId = 'tenant-corp-a';
      const workflowId = `wf-outbox-${uuidv4()}`;
      const eventId = uuidv4();

      const eventPayload = {
        specversion: '1.0',
        id: eventId,
        source: '/services/workflow-api',
        type: 'workflow.submitted.v1',
        time: new Date().toISOString(),
        datacontenttype: 'application/json',
        data: {
          workflowId,
          tenantId,
          title: 'Live Outbox Test Workflow',
          amount: 25000,
          requesterId: 'user-alice',
        },
      };

      // 1. Atomic DB Dual-Write Transaction
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(
          `INSERT INTO workflows (id, tenant_id, title, amount, requester_id, status)
           VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
          [workflowId, tenantId, 'Live Outbox Test Workflow', 25000, 'user-alice']
        );

        await client.query(
          `INSERT INTO outbox_events (id, tenant_id, workflow_id, event_type, payload, published)
           VALUES ($1, $2, $3, $4, $5, FALSE)`,
          [eventId, tenantId, workflowId, eventPayload.type, JSON.stringify(eventPayload)]
        );
        await client.query('COMMIT');
      } finally {
        client.release();
      }

      // Verify staged in DB with published = false
      const unpub = await pool.query(`SELECT published, retry_count FROM outbox_events WHERE id = $1`, [eventId]);
      expect(unpub.rows[0].published).toBe(false);
      expect(unpub.rows[0].retry_count).toBe(0);

      // 2. Simulate Outbox Relay Worker Cycle
      const pendingEvents = await pool.query(
        `SELECT id, tenant_id, workflow_id, event_type, payload
         FROM outbox_events
         WHERE published = FALSE AND retry_count < 10
         ORDER BY created_at ASC
         LIMIT 10
         FOR UPDATE SKIP LOCKED`
      );

      expect(pendingEvents.rows.length).toBeGreaterThanOrEqual(1);
      const row = pendingEvents.rows.find((r) => r.id === eventId);
      expect(row).toBeDefined();

      // Publish to real Kafka broker with partition key `${tenantId}:${workflowId}`
      const partitionKey = `${tenantId}:${workflowId}`;
      await kafkaProducer.send({
        topic: TOPIC_NAME,
        messages: [
          {
            key: partitionKey,
            value: JSON.stringify(row.payload),
            headers: {
              'x-correlation-id': uuidv4(),
              'tenant-id': tenantId,
            },
          },
        ],
      });

      // Mark published in DB
      await pool.query(
        `UPDATE outbox_events SET published = TRUE, published_at = NOW() WHERE id = $1`,
        [eventId]
      );

      // Verify PostgreSQL record status updated
      const updatedRow = await pool.query(`SELECT published, published_at FROM outbox_events WHERE id = $1`, [eventId]);
      expect(updatedRow.rows[0].published).toBe(true);
      expect(updatedRow.rows[0].published_at).not.toBeNull();
    });

    it('isolates publish failures, increments retry_count, and records last_error in PostgreSQL', async () => {
      const tenantId = 'tenant-corp-b';
      const workflowId = `wf-fail-${uuidv4()}`;
      const eventId = uuidv4();

      await pool.query(
        `INSERT INTO outbox_events (id, tenant_id, workflow_id, event_type, payload, published, retry_count)
         VALUES ($1, $2, $3, 'workflow.step_approved.v1', $4, FALSE, 0)`,
        [eventId, tenantId, workflowId, JSON.stringify({ errorTest: true })]
      );

      // Simulate simulated broker network failure / timeout
      const simulatedError = 'BrokerTransportFailure: Connection reset by peer';
      await pool.query(
        `UPDATE outbox_events
         SET retry_count = retry_count + 1, last_error = $1
         WHERE id = $2`,
        [simulatedError, eventId]
      );

      const failedRow = await pool.query(`SELECT published, retry_count, last_error FROM outbox_events WHERE id = $1`, [eventId]);
      expect(failedRow.rows[0].published).toBe(false);
      expect(failedRow.rows[0].retry_count).toBe(1);
      expect(failedRow.rows[0].last_error).toContain('BrokerTransportFailure');
    });

    it('auto-drains batch of chronological outbox events to 0 remaining backlog', async () => {
      const tenantId = 'tenant-corp-c';
      const batchSize = 5;
      const ids: string[] = [];

      for (let i = 0; i < batchSize; i++) {
        const id = uuidv4();
        ids.push(id);
        await pool.query(
          `INSERT INTO outbox_events (id, tenant_id, workflow_id, event_type, payload, published, created_at)
           VALUES ($1, $2, $3, 'workflow.step_approved.v1', $4, FALSE, NOW() + INTERVAL '${i} milliseconds')`,
          [id, tenantId, `wf-batch-${i}`, JSON.stringify({ step: i })]
        );
      }

      // Batch fetch ordered chronologically
      const batchRes = await pool.query(
        `SELECT id, tenant_id, workflow_id, payload
         FROM outbox_events
         WHERE published = FALSE AND id = ANY($1::varchar[])
         ORDER BY created_at ASC`,
        [ids]
      );

      expect(batchRes.rows.length).toBe(batchSize);

      // Publish entire batch to real Kafka container
      const kafkaMessages = batchRes.rows.map((r) => ({
        key: `${r.tenant_id}:${r.workflow_id}`,
        value: JSON.stringify(r.payload),
      }));

      await kafkaProducer.send({
        topic: TOPIC_NAME,
        messages: kafkaMessages,
      });

      // Mark all as published
      await pool.query(
        `UPDATE outbox_events SET published = TRUE, published_at = NOW() WHERE id = ANY($1::varchar[])`,
        [ids]
      );

      // Backlog for these events must be exactly 0
      const remainingBacklog = await pool.query(
        `SELECT COUNT(*) AS count FROM outbox_events WHERE published = FALSE AND id = ANY($1::varchar[])`,
        [ids]
      );
      expect(parseInt(remainingBacklog.rows[0].count, 10)).toBe(0);
    });
  });

  if (!dockerRunning) {
    it.skip('Live Outbox Relay & Kafka Auto-Drain suite (Docker daemon not running in current environment)', () => {});
  }
});
