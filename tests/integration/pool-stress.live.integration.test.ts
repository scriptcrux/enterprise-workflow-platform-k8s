import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { isDockerAvailable, startTestPostgres } from '@workflow/test-utils';
import pg from 'pg';
import { v4 as uuidv4 } from 'uuid';

describe('Live PostgreSQL High-Concurrency Connection Pool & Context Bleed Suite (Testcontainers)', async () => {
  const dockerRunning = await isDockerAvailable();

  describe.skipIf(!dockerRunning)('Connection Pool Multi-Tenant Stress Engine', () => {
    let containerRes: any;
    let pool: pg.Pool;
    let constrainedPool: pg.Pool;
    let stopContainer: () => Promise<void>;

    const tenants = ['tenant-alpha', 'tenant-beta', 'tenant-gamma', 'tenant-delta', 'tenant-omega'];

    beforeAll(async () => {
      containerRes = await startTestPostgres();
      pool = containerRes.pool;
      stopContainer = containerRes.stop;

      // 1. Initialize schema with strict RLS policies
      await pool.query(`
        CREATE TABLE IF NOT EXISTS workflows (
          id VARCHAR(64) PRIMARY KEY,
          tenant_id VARCHAR(64) NOT NULL,
          title VARCHAR(255) NOT NULL,
          amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
          requester_id VARCHAR(64) NOT NULL,
          status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        ALTER TABLE workflows ENABLE ROW LEVEL SECURITY;
        ALTER TABLE workflows FORCE ROW LEVEL SECURITY;

        DROP POLICY IF EXISTS workflow_tenant_isolation_policy ON workflows;
        CREATE POLICY workflow_tenant_isolation_policy ON workflows
          AS PERMISSIVE
          FOR ALL
          TO PUBLIC
          USING (
            current_setting('app.bypass_rls', true) = 'on'
            OR tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')
          )
          WITH CHECK (
            current_setting('app.bypass_rls', true) = 'on'
            OR tenant_id = NULLIF(current_setting('app.current_tenant_id', true), '')
          );
      `);

      // 2. Insert baseline records for each tenant using bypass RLS
      await pool.query(`
        BEGIN;
        SELECT set_config('app.bypass_rls', 'on', true);
        INSERT INTO workflows (id, tenant_id, title, amount, requester_id) VALUES
          ('wf-alpha-1', 'tenant-alpha', 'Alpha Cloud Spend', 10000, 'user-alpha'),
          ('wf-beta-1', 'tenant-beta', 'Beta Server Contract', 25000, 'user-beta'),
          ('wf-gamma-1', 'tenant-gamma', 'Gamma AI Compute', 40000, 'user-gamma'),
          ('wf-delta-1', 'tenant-delta', 'Delta Security Audit', 15000, 'user-delta'),
          ('wf-omega-1', 'tenant-omega', 'Omega Hardware Refresh', 80000, 'user-omega')
        ON CONFLICT (id) DO NOTHING;
        COMMIT;
      `);

      // 3. Create a highly constrained connection pool (max: 3 connections) to force aggressive reuse
      constrainedPool = new pg.Pool({
        connectionString: containerRes.connectionString,
        max: 3,
        idleTimeoutMillis: 1000,
      });
    }, 60000);

    afterAll(async () => {
      if (constrainedPool) {
        await constrainedPool.end();
      }
      if (stopContainer) {
        await stopContainer();
      }
    });

    it('prevents session context bleed across 25 concurrent tasks sharing a 3-connection pool', async () => {
      const taskCount = 25;
      const tasks = Array.from({ length: taskCount }, async (_, idx) => {
        const assignedTenant = tenants[idx % tenants.length];
        const client = await constrainedPool.connect();

        try {
          // Verify that connection starts with completely clean tenant context
          const preCheck = await client.query(`SELECT current_setting('app.current_tenant_id', true) AS tenant_id`);
          expect(preCheck.rows[0].tenant_id || '').toBe('');

          // Begin transaction with localized tenant session
          await client.query('BEGIN');
          await client.query(`SELECT set_config('app.current_tenant_id', $1, true)`, [assignedTenant]);

          // Query workflows under RLS
          const rowsRes = await client.query('SELECT * FROM workflows');

          // Strict assertion: Every returned row MUST strictly belong to the assigned tenant
          expect(rowsRes.rows.length).toBeGreaterThanOrEqual(1);
          for (const row of rowsRes.rows) {
            expect(row.tenant_id).toBe(assignedTenant);
          }

          // Simulate random async processing latency (10-30ms) to force connection queue contention
          await new Promise((r) => setTimeout(r, Math.floor(Math.random() * 20) + 10));

          await client.query('COMMIT');

          // Post-transaction verification: is_local = true must have wiped session context
          const postCheck = await client.query(`SELECT current_setting('app.current_tenant_id', true) AS tenant_id`);
          expect(postCheck.rows[0].tenant_id || '').toBe('');

          return { taskId: idx, tenant: assignedTenant, status: 'SUCCESS' };
        } finally {
          client.release();
        }
      });

      const results = await Promise.all(tasks);
      expect(results.length).toBe(taskCount);
      expect(results.every((r) => r.status === 'SUCCESS')).toBe(true);
    });

    it('cleanses tenant session context even when transactions fail abruptly with ROLLBACK or errors', async () => {
      const failureTaskCount = 15;
      const tasks = Array.from({ length: failureTaskCount }, async (_, idx) => {
        const assignedTenant = tenants[idx % tenants.length];
        const client = await constrainedPool.connect();

        try {
          await client.query('BEGIN');
          await client.query(`SELECT set_config('app.current_tenant_id', $1, true)`, [assignedTenant]);

          // Deliberate failure: syntax error or forced abort
          if (idx % 2 === 0) {
            await client.query('SELECT * FROM non_existent_table_to_force_error').catch(() => {});
          }

          // Abort and rollback
          await client.query('ROLLBACK');

          // Ensure session is pristine immediately after rollback
          const postRollbackCheck = await client.query(`SELECT current_setting('app.current_tenant_id', true) AS tenant_id`);
          expect(postRollbackCheck.rows[0].tenant_id || '').toBe('');

          return { taskId: idx, status: 'ROLLED_BACK_CLEANLY' };
        } finally {
          client.release();
        }
      });

      const results = await Promise.all(tasks);
      expect(results.every((r) => r.status === 'ROLLED_BACK_CLEANLY')).toBe(true);
    });

    it('ensures zero unhandled deadlocks or pool exhaustion under 50 rapid transactions', async () => {
      const totalOps = 50;
      let completedOps = 0;

      const promises = Array.from({ length: totalOps }, async (_, i) => {
        const tenant = tenants[i % tenants.length];
        const client = await constrainedPool.connect();
        try {
          await client.query('BEGIN');
          await client.query(`SELECT set_config('app.current_tenant_id', $1, true)`, [tenant]);
          const res = await client.query('SELECT COUNT(*) AS total FROM workflows');
          expect(parseInt(res.rows[0].total, 10)).toBe(1);
          await client.query('COMMIT');
          completedOps++;
        } finally {
          client.release();
        }
      });

      await Promise.all(promises);
      expect(completedOps).toBe(totalOps);
    });
  });

  if (!dockerRunning) {
    it.skip('Live PostgreSQL Pool Stress suite (Docker daemon not running in current environment)', () => {});
  }
});
