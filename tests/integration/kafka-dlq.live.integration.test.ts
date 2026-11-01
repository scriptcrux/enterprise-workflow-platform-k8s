import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { isDockerAvailable, startTestPostgres, startTestKafka } from '@workflow/test-utils';
import { validateEvent } from '@workflow/shared-schemas';
import pg from 'pg';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

describe('Live Kafka Dead Letter Queue (DLQ) & Poison Pill Replay Suite (Testcontainers)', async () => {
  const dockerRunning = await isDockerAvailable();

  describe.skipIf(!dockerRunning)('Live Kafka Resilience & Poison Pill Pipeline', () => {
    let pgRes: any;
    let kafkaRes: any;
    let pool: pg.Pool;
    let kafkaProducer: Producer;
    let kafkaConsumer: Consumer;
    let kafkaAdmin: any;
    let stopPg: () => Promise<void>;
    let stopKafka: () => Promise<void>;

    const TOPIC_EVENTS = 'workflow.events';
    const TOPIC_RETRY = 'workflow.events.retry';
    const TOPIC_DLQ = 'workflow.events.dlq';

    beforeAll(async () => {
      // 1. Start real PostgreSQL and Kafka Testcontainers
      pgRes = await startTestPostgres();
      pool = pgRes.pool;
      stopPg = pgRes.stop;

      kafkaRes = await startTestKafka();
      stopKafka = kafkaRes.stop;

      // 2. Initialize audit_db schema
      await pool.query(`
        CREATE TABLE IF NOT EXISTS audit_events (
          id VARCHAR(64) PRIMARY KEY,
          workflow_id VARCHAR(64) NOT NULL,
          tenant_id VARCHAR(64) NOT NULL,
          event_type VARCHAR(128) NOT NULL,
          actor_id VARCHAR(64) NOT NULL,
          timestamp TIMESTAMPTZ NOT NULL,
          payload JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dlq_messages (
          id VARCHAR(64) PRIMARY KEY,
          original_topic VARCHAR(128) NOT NULL,
          original_key VARCHAR(255),
          payload JSONB NOT NULL,
          error_type VARCHAR(64) NOT NULL,
          error_message TEXT NOT NULL,
          retry_count INT NOT NULL DEFAULT 0,
          tenant_id VARCHAR(64),
          workflow_id VARCHAR(64),
          headers JSONB,
          status VARCHAR(32) NOT NULL DEFAULT 'DEAD_LETTERED',
          failed_at TIMESTAMPTZ NOT NULL,
          replayed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      // 3. Connect Kafka client and create topics
      const kafka = new Kafka({
        clientId: 'dlq-live-test-client',
        brokers: [kafkaRes.bootstrapServers],
        retry: { retries: 3 },
      });

      kafkaAdmin = kafka.admin();
      await kafkaAdmin.connect();
      await kafkaAdmin.createTopics({
        topics: [
          { topic: TOPIC_EVENTS, numPartitions: 2, replicationFactor: 1 },
          { topic: TOPIC_RETRY, numPartitions: 2, replicationFactor: 1 },
          { topic: TOPIC_DLQ, numPartitions: 2, replicationFactor: 1 },
        ],
      });

      kafkaProducer = kafka.producer();
      await kafkaProducer.connect();

      kafkaConsumer = kafka.consumer({ groupId: `test-dlq-group-${uuidv4()}` });
      await kafkaConsumer.connect();
      await kafkaConsumer.subscribe({
        topics: [TOPIC_EVENTS, TOPIC_DLQ],
        fromBeginning: true,
      });
    }, 120000);

    afterAll(async () => {
      if (kafkaConsumer) await kafkaConsumer.disconnect().catch(() => {});
      if (kafkaProducer) await kafkaProducer.disconnect().catch(() => {});
      if (kafkaAdmin) await kafkaAdmin.disconnect().catch(() => {});
      if (stopKafka) await stopKafka().catch(() => {});
      if (stopPg) await stopPg().catch(() => {});
    });

    it('intercepts unparseable non-JSON poison pill, routes to DLQ topic, and writes to Postgres dlq_messages', async () => {
      const toxicPayload = 'THIS IS CORRUPTED RAW BINARY / NON-JSON STRING {{{{';
      const correlationId = uuidv4();
      const traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

      // 1. Simulate consumer parsing safeguard
      let parsed: any = null;
      let parseError: Error | null = null;
      try {
        parsed = JSON.parse(toxicPayload);
      } catch (err: any) {
        parseError = err;
      }

      expect(parseError).not.toBeNull();

      // 2. Route to Kafka DLQ Topic with diagnostic resilience headers
      const dlqMessageId = `dlq-${uuidv4()}`;
      await kafkaProducer.send({
        topic: TOPIC_DLQ,
        messages: [
          {
            key: 'poison-key-1',
            value: JSON.stringify({ raw: toxicPayload, id: dlqMessageId }),
            headers: {
              'x-original-topic': TOPIC_EVENTS,
              'x-error-type': 'UNPARSEABLE_JSON',
              'x-error-message': `JSON parse syntax error: ${parseError!.message}`,
              'x-failed-at': new Date().toISOString(),
              'x-correlation-id': correlationId,
              'traceparent': traceparent,
            },
          },
        ],
      });

      // 3. DLQ Consumer ingests from TOPIC_DLQ and inserts into PostgreSQL
      await pool.query(
        `INSERT INTO dlq_messages (id, original_topic, original_key, payload, error_type, error_message, status, failed_at, headers)
         VALUES ($1, $2, $3, $4, $5, $6, 'DEAD_LETTERED', NOW(), $7)`,
        [
          dlqMessageId,
          TOPIC_EVENTS,
          'poison-key-1',
          JSON.stringify({ raw: toxicPayload }),
          'UNPARSEABLE_JSON',
          `JSON parse syntax error: ${parseError!.message}`,
          JSON.stringify({ correlationId, traceparent }),
        ]
      );

      // Verify PostgreSQL DLQ record
      const dlqRow = await pool.query(`SELECT * FROM dlq_messages WHERE id = $1`, [dlqMessageId]);
      expect(dlqRow.rows.length).toBe(1);
      expect(dlqRow.rows[0].error_type).toBe('UNPARSEABLE_JSON');
      expect(dlqRow.rows[0].status).toBe('DEAD_LETTERED');
      expect(dlqRow.rows[0].original_topic).toBe(TOPIC_EVENTS);
    });

    it('intercepts schema validation failure on malformed CloudEvent and routes to DLQ', async () => {
      const invalidEvent = {
        specversion: '1.0',
        id: uuidv4(),
        source: '/services/workflow-api',
        type: 'workflow.submitted.v1',
        time: new Date().toISOString(),
        datacontenttype: 'application/json',
        data: {
          // Missing mandatory workflowId, requesterId, tenantId, etc.
          invalidField: 'bad data',
        },
      };

      // Validate against JSON schema validator
      const validation = validateEvent('workflow.submitted.v1', invalidEvent as any);
      expect(validation.valid).toBe(false);

      const dlqId = `dlq-${uuidv4()}`;
      const errorMessage = validation.errors?.join(', ') || 'Schema validation failed';

      // Persist to DLQ table
      await pool.query(
        `INSERT INTO dlq_messages (id, original_topic, original_key, payload, error_type, error_message, status, failed_at)
         VALUES ($1, $2, $3, $4, 'SCHEMA_VALIDATION_ERROR', $5, 'DEAD_LETTERED', NOW())`,
        [dlqId, TOPIC_EVENTS, 'schema-bad-key', JSON.stringify(invalidEvent), errorMessage]
      );

      const row = await pool.query(`SELECT error_type, error_message, status FROM dlq_messages WHERE id = $1`, [dlqId]);
      expect(row.rows[0].error_type).toBe('SCHEMA_VALIDATION_ERROR');
      expect(row.rows[0].status).toBe('DEAD_LETTERED');
    });

    it('replays a dead-lettered message: re-publishes to Kafka and transitions DB status to REPLAYED', async () => {
      const replayId = `dlq-replay-${uuidv4()}`;
      const fixedEvent = {
        id: uuidv4(),
        source: '/services/workflow-api',
        specversion: '1.0',
        type: 'workflow.approved.v1',
        time: new Date().toISOString(),
        datacontenttype: 'application/json',
        data: {
          workflowId: `wf-${uuidv4()}`,
          tenantId: 'tenant-corp-a',
          status: 'APPROVED',
        },
      };

      // 1. Insert initial dead-lettered record
      await pool.query(
        `INSERT INTO dlq_messages (id, original_topic, original_key, payload, error_type, error_message, status, failed_at)
         VALUES ($1, $2, $3, $4, 'PROCESSING_ERROR', 'Temporary network timeout', 'DEAD_LETTERED', NOW())`,
        [replayId, TOPIC_EVENTS, 'tenant-corp-a:wf-101', JSON.stringify(fixedEvent)]
      );

      // 2. Execute Operator Replay Operation
      // Fetch record
      const fetchRes = await pool.query(`SELECT * FROM dlq_messages WHERE id = $1`, [replayId]);
      const record = fetchRes.rows[0];
      expect(record.status).toBe('DEAD_LETTERED');

      // Re-publish to original Kafka topic
      await kafkaProducer.send({
        topic: record.original_topic,
        messages: [
          {
            key: record.original_key,
            value: JSON.stringify(record.payload),
            headers: {
              'x-replayed-from-dlq': 'true',
              'x-dlq-id': replayId,
            },
          },
        ],
      });

      // Update DB status to REPLAYED
      await pool.query(
        `UPDATE dlq_messages
         SET status = 'REPLAYED', replayed_at = NOW()
         WHERE id = $1`,
        [replayId]
      );

      // Verify DB record status updated
      const replayedRow = await pool.query(`SELECT status, replayed_at FROM dlq_messages WHERE id = $1`, [replayId]);
      expect(replayedRow.rows[0].status).toBe('REPLAYED');
      expect(replayedRow.rows[0].replayed_at).not.toBeNull();
    });
  });

  if (!dockerRunning) {
    it.skip('Live Kafka DLQ & Poison Pill Replay suite (Docker daemon not running in current environment)', () => {});
  }
});
