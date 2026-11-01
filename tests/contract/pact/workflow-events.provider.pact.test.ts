import { describe, it, expect } from 'vitest';
import { Verifier } from '@pact-foundation/pact';
import path from 'path';
import { buildWorkflowEvent } from '@workflow/test-utils';

describe('Pact Message Provider Verification: workflow-api satisfies async event contracts', () => {
  it('verifies notification-service message contract', async () => {
    const pactFile = path.resolve(process.cwd(), 'pacts', 'notification-service-workflow-platform-events.json');

    const verifier = new Verifier({
      provider: 'workflow-platform-events',
      pactUrls: [pactFile],
      messageProviders: {
        'a CloudEvent for workflow.submitted.v1': () =>
          Promise.resolve(
            buildWorkflowEvent(
              'workflow.submitted.v1',
              {
                workflowId: 'e1a2b3c4-d5e6-7f80-1234-56789abcdef0',
                tenantId: 'tenant-corp-a',
                type: 'EXPENSE',
                title: 'AWS Cloud Infrastructure Annual Invoice',
                requesterId: 'user-alice',
                requesterName: 'Alice Engineer',
                actorId: 'user-alice',
                previousStatus: 'DRAFT',
                currentStatus: 'PENDING',
                stepRole: 'APPROVER',
                amount: 12000,
                currency: 'USD',
                timestamp: '2026-09-21T08:00:00.000Z',
              },
              {
                id: 'f1e2d3c4-b5a6-7890-1234-56789abcdef0',
                time: '2026-09-21T08:00:00.000Z',
              }
            )
          ),
        'a CloudEvent for workflow.step_approved.v1': () =>
          Promise.resolve(
            buildWorkflowEvent(
              'workflow.step_approved.v1',
              {
                workflowId: 'e1a2b3c4-d5e6-7f80-1234-56789abcdef0',
                tenantId: 'tenant-corp-a',
                type: 'EXPENSE',
                title: 'AWS Cloud Infrastructure Annual Invoice',
                requesterId: 'user-alice',
                requesterName: 'Alice Engineer',
                actorId: 'user-lead-reviewer',
                previousStatus: 'PENDING',
                currentStatus: 'PENDING',
                currentStepOrder: 1,
                totalSteps: 2,
                timestamp: '2026-09-21T08:02:00.000Z',
              },
              {
                id: 'd1e2d3c4-b5a6-7890-1234-56789abcdef3',
                time: '2026-09-21T08:02:00.000Z',
              }
            )
          ),
        'a CloudEvent for workflow.approved.v1': () =>
          Promise.resolve(
            buildWorkflowEvent(
              'workflow.approved.v1',
              {
                workflowId: 'e1a2b3c4-d5e6-7f80-1234-56789abcdef0',
                tenantId: 'tenant-corp-a',
                type: 'EXPENSE',
                title: 'AWS Cloud Infrastructure Annual Invoice',
                requesterId: 'user-alice',
                requesterName: 'Alice Engineer',
                actorId: 'user-bob-mgr',
                previousStatus: 'PENDING',
                currentStatus: 'APPROVED',
                comment: 'Approved based on FY26 Cloud budget line item',
                timestamp: '2026-09-21T08:05:00.000Z',
              },
              {
                id: 'b1e2d3c4-b5a6-7890-1234-56789abcdef1',
                time: '2026-09-21T08:05:00.000Z',
              }
            )
          ),
        'a CloudEvent for workflow.cancelled.v1': () =>
          Promise.resolve(
            buildWorkflowEvent(
              'workflow.cancelled.v1',
              {
                workflowId: 'e1a2b3c4-d5e6-7f80-1234-56789abcdef0',
                tenantId: 'tenant-corp-a',
                type: 'EXPENSE',
                title: 'Obsolete Hardware Request',
                requesterId: 'user-alice',
                requesterName: 'Alice Engineer',
                actorId: 'user-alice',
                previousStatus: 'PENDING',
                currentStatus: 'CANCELLED',
                timestamp: '2026-09-21T08:12:00.000Z',
              },
              {
                id: 'e1e2d3c4-b5a6-7890-1234-56789abcdef4',
                time: '2026-09-21T08:12:00.000Z',
              }
            )
          ),
      },
    });

    const output = await verifier.verifyProvider();
    expect(output).toBeDefined();
  });

  it('verifies audit-service message contract', async () => {
    const pactFile = path.resolve(process.cwd(), 'pacts', 'audit-service-workflow-platform-events.json');

    const verifier = new Verifier({
      provider: 'workflow-platform-events',
      pactUrls: [pactFile],
      messageProviders: {
        'a CloudEvent for workflow.rejected.v1': () =>
          Promise.resolve(
            buildWorkflowEvent(
              'workflow.rejected.v1',
              {
                workflowId: 'e1a2b3c4-d5e6-7f80-1234-56789abcdef0',
                tenantId: 'tenant-corp-a',
                type: 'EXPENSE',
                title: 'Unapproved Software License',
                requesterId: 'user-alice',
                requesterName: 'Alice Engineer',
                actorId: 'user-bob-mgr',
                previousStatus: 'PENDING',
                currentStatus: 'REJECTED',
                rejectionReason: 'Exceeds team quarterly discretionary budget',
                timestamp: '2026-09-21T08:10:00.000Z',
              },
              {
                id: 'c1e2d3c4-b5a6-7890-1234-56789abcdef2',
                time: '2026-09-21T08:10:00.000Z',
              }
            )
          ),
      },
    });

    const output = await verifier.verifyProvider();
    expect(output).toBeDefined();
  });
});
