import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { isDockerAvailable, startTestPostgres } from '@workflow/test-utils';
import pg from 'pg';
import { v4 as uuidv4 } from 'uuid';

describe('Live PostgreSQL Concurrent Idempotency & Atomic Locking Suite (Testcontainers)', async () => {
  const dockerRunning = await isDockerAvailable();

  describe.skipIf(!dockerRunning)('Live Database Idempotency Locking Engine', () => {
    let pool: pg.Pool;
    let stopContainer: () => Promise<void>;

    beforeAll(async () => {
      const res = await startTestPostgres();
      pool = res.pool;
      stopContainer = res.stop;

      // Initialize real PostgreSQL schema
      await pool.query(`
        CREATE TABLE IF NOT EXISTS workflows (
          id VARCHAR(64) PRIMARY KEY,
          tenant_id VARCHAR(64) NOT NULL,
          type VARCHAR(64) NOT NULL DEFAULT 'EXPENSE',
          title VARCHAR(255) NOT NULL,
          amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
          currency VARCHAR(3) NOT NULL DEFAULT 'USD',
          requester_id VARCHAR(64) NOT NULL,
          requester_name VARCHAR(128) NOT NULL DEFAULT 'Requester',
          status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
          current_step_order INT NOT NULL DEFAULT 1,
          total_steps INT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS idempotency_keys (
          id VARCHAR(64) PRIMARY KEY,
          tenant_id VARCHAR(64) NOT NULL,
          key VARCHAR(255) NOT NULL,
          status_code INT,
          response_body JSONB,
          status VARCHAR(32) NOT NULL DEFAULT 'PROCESSING',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT uk_idempotency_tenant_key UNIQUE (tenant_id, key)
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
    }, 60000);

    afterAll(async () => {
      if (stopContainer) {
        await stopContainer();
      }
    });

    it('guarantees atomic locking: 10 concurrent requests with the same Idempotency-Key result in exactly 1 write', async () => {
      const tenantId = 'tenant-corp-a';
      const sharedKey = `idemp-race-${uuidv4()}`;
      const workflowId = `wf-race-${uuidv4()}`;

      // Insert initial workflow
      await pool.query(
        `INSERT INTO workflows (id, tenant_id, title, amount, requester_id, status)
         VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
        [workflowId, tenantId, 'Concurrent Race Test Expense', 50000, 'user-alice']
      );

      // Simulate 10 simultaneous workers racing to process the approval step
      const concurrencyCount = 10;
      const workerPromises = Array.from({ length: concurrencyCount }, async (_, idx) => {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');

          // Attempt to claim the idempotency lock atomically
          const lockResult = await client.query(
            `INSERT INTO idempotency_keys (id, tenant_id, key, status)
             VALUES ($1, $2, $3, 'PROCESSING')
             ON CONFLICT (tenant_id, key) DO NOTHING
             RETURNING id`,
            [`idemp-rec-${uuidv4()}`, tenantId, sharedKey]
          );

          if (lockResult.rowCount === 1) {
            // Winner: Transition workflow state and create outbox event
            await client.query(
              `UPDATE workflows SET status = 'APPROVED', updated_at = NOW() WHERE id = $1`,
              [workflowId]
            );

            const responsePayload = {
              workflowId,
              status: 'APPROVED',
              approvedBy: `worker-${idx}`,
            };

            await client.query(
              `INSERT INTO outbox_events (id, tenant_id, workflow_id, event_type, payload)
               VALUES ($1, $2, $3, 'workflow.approved.v1', $4)`,
              [uuidv4(), tenantId, workflowId, JSON.stringify(responsePayload)]
            );

            // Mark idempotency key as COMPLETED
            await client.query(
              `UPDATE idempotency_keys
               SET status = 'COMPLETED', status_code = 200, response_body = $1, updated_at = NOW()
               WHERE tenant_id = $2 AND key = $3`,
              [JSON.stringify(responsePayload), tenantId, sharedKey]
            );

            await client.query('COMMIT');
            return { workerId: idx, wonLock: true, isReplay: false };
          } else {
            // Loser: Fetch cached idempotency record
            await client.query('COMMIT');
            const cachedRecord = await pool.query(
              `SELECT status, status_code, response_body FROM idempotency_keys WHERE tenant_id = $1 AND key = $2`,
              [tenantId, sharedKey]
            );
            return {
              workerId: idx,
              wonLock: false,
              isReplay: true,
              status: cachedRecord.rows[0]?.status,
            };
          }
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        } finally {
          client.release();
        }
      });

      const results = await Promise.all(workerPromises);

      // Verify that EXACTLY 1 worker won the race
      const winners = results.filter((r) => r.wonLock);
      const losers = results.filter((r) => !r.wonLock);

      expect(winners.length).toBe(1);
      expect(losers.length).toBe(concurrencyCount - 1);

      // Verify only 1 outbox event was generated
      const outboxRows = await pool.query(
        `SELECT * FROM outbox_events WHERE workflow_id = $1`,
        [workflowId]
      );
      expect(outboxRows.rows.length).toBe(1);

      // Verify workflow status is APPROVED
      const workflowRow = await pool.query(
        `SELECT status FROM workflows WHERE id = $1`,
        [workflowId]
      );
      expect(workflowRow.rows[0].status).toBe('APPROVED');
    });

    it('isolates idempotency keys across tenants: identical keys do not collide between tenant-alpha and tenant-beta', async () => {
      const sharedKey = `idemp-multi-tenant-${uuidv4()}`;

      // Tenant Alpha inserts key
      const resAlpha = await pool.query(
        `INSERT INTO idempotency_keys (id, tenant_id, key, status_code, response_body, status)
         VALUES ($1, 'tenant-alpha', $2, 200, $3, 'COMPLETED')
         ON CONFLICT (tenant_id, key) DO NOTHING
         RETURNING id`,
        [uuidv4(), sharedKey, JSON.stringify({ message: 'Response for Alpha' })]
      );

      // Tenant Beta inserts identical key
      const resBeta = await pool.query(
        `INSERT INTO idempotency_keys (id, tenant_id, key, status_code, response_body, status)
         VALUES ($1, 'tenant-beta', $2, 200, $3, 'COMPLETED')
         ON CONFLICT (tenant_id, key) DO NOTHING
         RETURNING id`,
        [uuidv4(), sharedKey, JSON.stringify({ message: 'Response for Beta' })]
      );

      // Both must successfully insert without collision
      expect(resAlpha.rowCount).toBe(1);
      expect(resBeta.rowCount).toBe(1);

      // Verify records are independent
      const alphaRow = await pool.query(
        `SELECT response_body FROM idempotency_keys WHERE tenant_id = 'tenant-alpha' AND key = $1`,
        [sharedKey]
      );
      const betaRow = await pool.query(
        `SELECT response_body FROM idempotency_keys WHERE tenant_id = 'tenant-beta' AND key = $1`,
        [sharedKey]
      );

      expect(alphaRow.rows[0].response_body.message).toBe('Response for Alpha');
      expect(betaRow.rows[0].response_body.message).toBe('Response for Beta');
    });
  });

  if (!dockerRunning) {
    it.skip('Live PostgreSQL Idempotency suite (Docker daemon not running in current environment)', () => {});
  }
});
