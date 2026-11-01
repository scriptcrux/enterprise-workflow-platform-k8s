import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Verifier } from '@pact-foundation/pact';
import path from 'path';

// State variables for dynamic state handling
let currentWorkflows: any[] = [];
let currentWorkflow: any = null;

const defaultMockWorkflow = {
  id: 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
  tenantId: 'tenant-corp-a',
  type: 'EXPENSE',
  title: 'MacBook Pro M3 Max for Development',
  amount: 3500,
  currency: 'USD',
  description: 'Team workstation refresh',
  status: 'DRAFT',
  currentStep: 1,
  totalSteps: 1,
  requesterId: 'user-alice',
  requesterName: 'Alice Requester',
  createdAt: '2026-09-21T08:00:00.000Z',
  updatedAt: '2026-09-21T08:00:00.000Z',
};

// Mock DB pool and services before importing app
vi.mock('../../../services/workflow-api/src/db/client.js', () => ({
  db: {
    insert: () => ({ values: () => Promise.resolve() }),
    select: () => ({
      from: () => ({
        where: () => ({
          limit: () => Promise.resolve([]),
          orderBy: () => Promise.resolve([]),
        }),
      }),
    }),
    update: () => ({ set: () => ({ where: () => Promise.resolve() }) }),
  },
  pool: {
    query: () => Promise.resolve({ rows: [] }),
    end: () => Promise.resolve(),
  },
}));

vi.mock('../../../services/workflow-api/src/services/workflow.service.js', () => ({
  workflowService: {
    createWorkflow: vi.fn().mockImplementation((tenantId, requesterId, requesterName, dto) =>
      Promise.resolve({
        id: 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
        tenantId,
        type: dto.type || 'EXPENSE',
        title: dto.title,
        amount: dto.amount || 3500,
        currency: dto.currency || 'USD',
        description: dto.description || 'Team workstation refresh',
        status: 'DRAFT',
        currentStep: 1,
        totalSteps: 1,
        requesterId,
        requesterName,
        createdAt: '2026-09-21T08:00:00.000Z',
        updatedAt: '2026-09-21T08:00:00.000Z',
      })
    ),
    listWorkflows: vi.fn().mockImplementation((tenantId, query) =>
      Promise.resolve({
        workflows: currentWorkflows,
        total: currentWorkflows.length,
        limit: 50,
        offset: 0,
      })
    ),
    getWorkflowById: vi.fn().mockImplementation((tenantId, id) =>
      Promise.resolve(id === 'f9e8d7c6-b5a4-3210-9876-543210fedcba' ? null : currentWorkflow || defaultMockWorkflow)
    ),
    submitWorkflow: vi.fn().mockImplementation((tenantId, userId, userName, id, body) =>
      Promise.resolve({
        id,
        tenantId,
        status: 'PENDING',
        currentStep: 1,
        totalSteps: 1,
        assignedRole: 'APPROVER',
        updatedAt: '2026-09-21T08:05:00.000Z',
      })
    ),
    approveWorkflow: vi.fn().mockImplementation((tenantId, userId, userName, id, body) =>
      Promise.resolve({
        id,
        tenantId,
        status: 'APPROVED',
        currentStep: 1,
        totalSteps: 1,
        updatedAt: '2026-09-21T08:10:00.000Z',
      })
    ),
    rejectWorkflow: vi.fn().mockImplementation((tenantId, userId, userName, id, body) =>
      Promise.resolve({
        id,
        tenantId,
        status: 'REJECTED',
        rejectionReason: body?.reason || 'Exceeds department budget limit',
        updatedAt: '2026-09-21T08:15:00.000Z',
      })
    ),
  },
}));

import { buildApp } from '../../../services/workflow-api/src/app.js';
import type { FastifyInstance } from 'fastify';

describe('Pact Provider Verification: workflow-api satisfies workflow-host-app contract', () => {
  let app: FastifyInstance;
  let providerBaseUrl: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.listen({ port: 0, host: '127.0.0.1' });
    const address = app.server.address();
    if (address && typeof address === 'object') {
      providerBaseUrl = `http://127.0.0.1:${address.port}`;
    } else {
      throw new Error('Could not determine provider port');
    }
  });

  afterAll(async () => {
    await app.close();
  });

  it('verifies provider against generated host-app pact file', async () => {
    const pactFile = path.resolve(process.cwd(), 'pacts', 'workflow-host-app-workflow-api.json');

    const verifier = new Verifier({
      provider: 'workflow-api',
      providerBaseUrl,
      pactUrls: [pactFile],
      stateHandlers: {
        'workflow creation is allowed': async () => {
          return Promise.resolve();
        },
        'invalid draft workflow payload': async () => {
          return Promise.resolve();
        },
        'workflows exist': async () => {
          currentWorkflows = [defaultMockWorkflow];
          return Promise.resolve();
        },
        'no workflows exist': async () => {
          currentWorkflows = [];
          return Promise.resolve();
        },
        'workflow exists': async () => {
          currentWorkflow = defaultMockWorkflow;
          return Promise.resolve();
        },
        'workflow does not exist': async () => {
          currentWorkflow = null;
          return Promise.resolve();
        },
        'workflow exists and is pending': async () => {
          currentWorkflow = { ...defaultMockWorkflow, status: 'PENDING' };
          return Promise.resolve();
        },
      },
    });

    const output = await verifier.verifyProvider();
    expect(output).toBeDefined();
  });
});
