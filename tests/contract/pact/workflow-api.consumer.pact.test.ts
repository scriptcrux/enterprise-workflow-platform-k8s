import { describe, it, expect } from 'vitest';
import { PactV3, MatchersV3 } from '@pact-foundation/pact';
import path from 'path';

const { like, string, uuid, integer, boolean } = MatchersV3;

const provider = new PactV3({
  consumer: 'workflow-host-app',
  provider: 'workflow-api',
  dir: path.resolve(process.cwd(), 'pacts'),
});

describe('Pact Consumer Contract: workflow-host-app -> workflow-api', () => {
  const tenantId = 'tenant-corp-a';
  const userId = 'user-alice';
  const workflowId = 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d';
  const nonExistentId = 'f9e8d7c6-b5a4-3210-9876-543210fedcba';

  it('generates contract for POST /api/v1/workflows (Create Draft - Success)', async () => {
    await provider
      .given('workflow creation is allowed')
      .uponReceiving('a request to create a draft workflow')
      .withRequest({
        method: 'POST',
        path: '/api/v1/workflows',
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': tenantId,
          'x-user-id': userId,
          'x-user-name': 'Alice Requester',
        },
        body: {
          type: 'EXPENSE',
          title: 'MacBook Pro M3 Max for Development',
          amount: 3500,
          currency: 'USD',
          description: 'Team workstation refresh',
        },
      })
      .willRespondWith({
        status: 201,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            id: uuid(workflowId),
            tenantId: string(tenantId),
            type: string('EXPENSE'),
            title: string('MacBook Pro M3 Max for Development'),
            amount: integer(3500),
            currency: string('USD'),
            description: string('Team workstation refresh'),
            status: string('DRAFT'),
            currentStep: integer(1),
            totalSteps: integer(1),
            requesterId: string(userId),
            requesterName: string('Alice Requester'),
            createdAt: string('2026-09-21T08:00:00.000Z'),
            updatedAt: string('2026-09-21T08:00:00.000Z'),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-user-id': userId,
            'x-user-name': 'Alice Requester',
          },
          body: JSON.stringify({
            type: 'EXPENSE',
            title: 'MacBook Pro M3 Max for Development',
            amount: 3500,
            currency: 'USD',
            description: 'Team workstation refresh',
          }),
        });

        expect(res.status).toBe(201);
        const body = (await res.json()) as { success: boolean; data: { id: string; status: string } };
        expect(body.success).toBe(true);
        expect(body.data.id).toBeDefined();
        expect(body.data.status).toBe('DRAFT');
      });
  });

  it('generates contract for POST /api/v1/workflows (Validation Error - 400)', async () => {
    await provider
      .given('invalid draft workflow payload')
      .uponReceiving('a request to create a draft workflow without a title')
      .withRequest({
        method: 'POST',
        path: '/api/v1/workflows',
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': tenantId,
          'x-user-id': userId,
        },
        body: {
          type: 'EXPENSE',
          amount: 3500,
        },
      })
      .willRespondWith({
        status: 400,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: false,
          error: {
            code: string('VALIDATION_ERROR'),
            message: string('Workflow title is required'),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-user-id': userId,
          },
          body: JSON.stringify({
            type: 'EXPENSE',
            amount: 3500,
          }),
        });

        expect(res.status).toBe(400);
        const body = (await res.json()) as { success: boolean; error: { code: string; message: string } };
        expect(body.success).toBe(false);
        expect(body.error.code).toBe('VALIDATION_ERROR');
      });
  });

  it('generates contract for GET /api/v1/workflows (List Workflows - Populated List)', async () => {
    await provider
      .given('workflows exist')
      .uponReceiving('a request to list all workflows')
      .withRequest({
        method: 'GET',
        path: '/api/v1/workflows',
        headers: {
          'x-tenant-id': tenantId,
          'x-user-id': userId,
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            workflows: [
              {
                id: uuid(workflowId),
                tenantId: string(tenantId),
                type: string('EXPENSE'),
                title: string('MacBook Pro M3 Max for Development'),
                status: string('DRAFT'),
                amount: integer(3500),
                currency: string('USD'),
                requesterId: string(userId),
              },
            ],
            total: integer(1),
            limit: integer(50),
            offset: integer(0),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows`, {
          method: 'GET',
          headers: {
            'x-tenant-id': tenantId,
            'x-user-id': userId,
          },
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { workflows: any[]; total: number } };
        expect(body.success).toBe(true);
        expect(body.data.workflows.length).toBeGreaterThan(0);
        expect(body.data.total).toBe(1);
      });
  });

  it('generates contract for GET /api/v1/workflows (List Workflows - Empty Array 200)', async () => {
    await provider
      .given('no workflows exist')
      .uponReceiving('a request to list workflows when none exist')
      .withRequest({
        method: 'GET',
        path: '/api/v1/workflows',
        headers: {
          'x-tenant-id': tenantId,
          'x-user-id': userId,
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            workflows: [],
            total: integer(0),
            limit: integer(50),
            offset: integer(0),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows`, {
          method: 'GET',
          headers: {
            'x-tenant-id': tenantId,
            'x-user-id': userId,
          },
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { workflows: any[]; total: number } };
        expect(body.success).toBe(true);
        expect(Array.isArray(body.data.workflows)).toBe(true);
        expect(body.data.workflows.length).toBe(0);
        expect(body.data.total).toBe(0);
      });
  });

  it('generates contract for GET /api/v1/workflows/:id (Query Existing Workflow)', async () => {
    await provider
      .given('workflow exists')
      .uponReceiving('a request to fetch an existing workflow by ID')
      .withRequest({
        method: 'GET',
        path: `/api/v1/workflows/${workflowId}`,
        headers: {
          'x-tenant-id': tenantId,
          'x-user-id': userId,
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            id: uuid(workflowId),
            tenantId: string(tenantId),
            type: string('EXPENSE'),
            title: string('MacBook Pro M3 Max for Development'),
            status: string('DRAFT'),
            amount: integer(3500),
            currency: string('USD'),
            requesterId: string(userId),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows/${workflowId}`, {
          method: 'GET',
          headers: {
            'x-tenant-id': tenantId,
            'x-user-id': userId,
          },
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { id: string; status: string } };
        expect(body.success).toBe(true);
        expect(body.data.id).toBe(workflowId);
      });
  });

  it('generates contract for GET /api/v1/workflows/:id (Workflow Not Found - 404)', async () => {
    await provider
      .given('workflow does not exist')
      .uponReceiving('a request to fetch a non-existent workflow by ID')
      .withRequest({
        method: 'GET',
        path: `/api/v1/workflows/${nonExistentId}`,
        headers: {
          'x-tenant-id': tenantId,
          'x-user-id': userId,
        },
      })
      .willRespondWith({
        status: 404,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: false,
          error: {
            code: string('WORKFLOW_NOT_FOUND'),
            message: string(`Workflow ${nonExistentId} not found`),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows/${nonExistentId}`, {
          method: 'GET',
          headers: {
            'x-tenant-id': tenantId,
            'x-user-id': userId,
          },
        });

        expect(res.status).toBe(404);
        const body = (await res.json()) as { success: boolean; error: { code: string; message: string } };
        expect(body.success).toBe(false);
        expect(body.error.code).toBe('WORKFLOW_NOT_FOUND');
      });
  });

  it('generates contract for POST /api/v1/workflows/:id/submit (Submit for Approval)', async () => {
    await provider
      .given('workflow exists')
      .uponReceiving('a request to submit a draft workflow for approval')
      .withRequest({
        method: 'POST',
        path: `/api/v1/workflows/${workflowId}/submit`,
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': tenantId,
          'x-user-id': userId,
          'x-user-name': 'Alice Requester',
        },
        body: {
          comment: 'Submitting hardware purchase for approval',
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            id: uuid(workflowId),
            tenantId: string(tenantId),
            status: string('PENDING'),
            currentStep: integer(1),
            totalSteps: integer(1),
            assignedRole: string('APPROVER'),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows/${workflowId}/submit`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-user-id': userId,
            'x-user-name': 'Alice Requester',
          },
          body: JSON.stringify({
            comment: 'Submitting hardware purchase for approval',
          }),
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { status: string } };
        expect(body.success).toBe(true);
        expect(body.data.status).toBe('PENDING');
      });
  });

  it('generates contract for POST /api/v1/workflows/:id/approve (Approve Workflow)', async () => {
    await provider
      .given('workflow exists and is pending')
      .uponReceiving('a request to approve a pending workflow')
      .withRequest({
        method: 'POST',
        path: `/api/v1/workflows/${workflowId}/approve`,
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': tenantId,
          'x-user-id': 'user-bob-mgr',
          'x-user-name': 'Bob Manager',
        },
        body: {
          comment: 'Approved for team workstation upgrade',
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            id: uuid(workflowId),
            tenantId: string(tenantId),
            status: string('APPROVED'),
            currentStep: integer(1),
            totalSteps: integer(1),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows/${workflowId}/approve`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-user-id': 'user-bob-mgr',
            'x-user-name': 'Bob Manager',
          },
          body: JSON.stringify({
            comment: 'Approved for team workstation upgrade',
          }),
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { status: string } };
        expect(body.success).toBe(true);
        expect(body.data.status).toBe('APPROVED');
      });
  });

  it('generates contract for POST /api/v1/workflows/:id/reject (Reject Workflow)', async () => {
    await provider
      .given('workflow exists and is pending')
      .uponReceiving('a request to reject a pending workflow')
      .withRequest({
        method: 'POST',
        path: `/api/v1/workflows/${workflowId}/reject`,
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': tenantId,
          'x-user-id': 'user-bob-mgr',
          'x-user-name': 'Bob Manager',
        },
        body: {
          reason: 'Exceeds department budget limit',
        },
      })
      .willRespondWith({
        status: 200,
        headers: {
          'content-type': 'application/json; charset=utf-8',
        },
        body: {
          success: true,
          data: {
            id: uuid(workflowId),
            tenantId: string(tenantId),
            status: string('REJECTED'),
            rejectionReason: string('Exceeds department budget limit'),
          },
        },
      })
      .executeTest(async (mockServer) => {
        const res = await fetch(`${mockServer.url}/api/v1/workflows/${workflowId}/reject`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-tenant-id': tenantId,
            'x-user-id': 'user-bob-mgr',
            'x-user-name': 'Bob Manager',
          },
          body: JSON.stringify({
            reason: 'Exceeds department budget limit',
          }),
        });

        expect(res.status).toBe(200);
        const body = (await res.json()) as { success: boolean; data: { status: string } };
        expect(body.success).toBe(true);
        expect(body.data.status).toBe('REJECTED');
      });
  });
});
