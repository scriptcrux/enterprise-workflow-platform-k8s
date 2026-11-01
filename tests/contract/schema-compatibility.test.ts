import { describe, it, expect } from 'vitest';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import { validateEvent, workflowApprovedSchema, workflowSubmittedSchema } from '../../packages/shared-schemas/src/validator.js';
import { buildWorkflowEvent } from '../../packages/test-utils/src/factories/event.factory.js';

describe('Real-World Kafka Schema Compatibility & Evolution Suite', () => {
  const ajv = new (Ajv as any)({ allErrors: true, strict: false });
  (addFormats as any)(ajv);

  describe('1. Baseline CloudEvents 1.0 Schema Validation', () => {
    it('validates a correct workflow.submitted.v1 event payload', () => {
      const event = buildWorkflowEvent('workflow.submitted.v1');
      const result = validateEvent('workflow.submitted.v1', event);

      expect(result.valid).toBe(true);
      expect(result.errors).toBeUndefined();
    });

    it('validates a correct workflow.approved.v1 event payload', () => {
      const event = buildWorkflowEvent('workflow.approved.v1');
      const result = validateEvent('workflow.approved.v1', event);

      expect(result.valid).toBe(true);
    });

    it('validates a correct workflow.rejected.v1 event payload with rejectionReason', () => {
      const event = buildWorkflowEvent('workflow.rejected.v1', {
        rejectionReason: 'Exceeds department quarterly budget ceiling',
      });
      const result = validateEvent('workflow.rejected.v1', event);

      expect(result.valid).toBe(true);
    });
  });

  describe('2. Backward Schema Compatibility Matrix (Safe Production Evolution)', () => {
    it('allows additive optional metadata extensions without breaking consumers', () => {
      const event = buildWorkflowEvent('workflow.submitted.v1', {
        metadata: {
          costCenter: 'CC-908',
          projectCode: 'PROJ-ALPHA',
          customTag: 12345,
          complianceCategory: 'FINANCIAL_EXPENSE',
        },
      });

      const result = validateEvent('workflow.submitted.v1', event);
      expect(result.valid).toBe(true);
    });

    it('allows adding new optional fields to domain data payload', () => {
      const v2ExtendedPayload = {
        ...buildWorkflowEvent('workflow.approved.v1'),
        data: {
          ...buildWorkflowEvent('workflow.approved.v1').data,
          taxDeductionRate: 0.08,
          exchangeRateUSD: 1.0,
        },
      };

      const result = validateEvent('workflow.approved.v1', v2ExtendedPayload);
      expect(result.valid).toBe(true);
    });
  });

  describe('3. Breaking Change Detection Matrix (Must Fail in CI Gate)', () => {
    it('fails when mandatory CloudEvents envelope attributes are omitted', () => {
      const event: any = buildWorkflowEvent('workflow.submitted.v1');
      delete event.specversion;

      const result = validateEvent('workflow.submitted.v1', event);
      expect(result.valid).toBe(false);
      expect(result.errors?.[0]).toContain('must have required property');
    });

    it('fails when mandatory domain identifiers (tenantId, workflowId) are missing', () => {
      const event: any = buildWorkflowEvent('workflow.submitted.v1');
      delete event.data.tenantId;

      const result = validateEvent('workflow.submitted.v1', event);
      expect(result.valid).toBe(false);
      expect(result.errors).toBeDefined();
    });

    it('rejects field renaming (e.g. title -> workflowTitle) as a breaking change', () => {
      const rawEvent: any = buildWorkflowEvent('workflow.submitted.v1');
      rawEvent.data.workflowTitle = rawEvent.data.title;
      delete rawEvent.data.title; // Renamed mandatory property

      // Schema expecting 'title' must fail
      const result = validateEvent('workflow.submitted.v1', rawEvent);
      expect(result.valid).toBe(false);
      expect(result.errors?.some((e) => e.includes('title'))).toBe(true);
    });

    it('rejects field type mutations (e.g. amount string instead of number)', () => {
      const rawEvent: any = buildWorkflowEvent('workflow.submitted.v1');
      rawEvent.data.amount = 'FIFTY_THOUSAND_DOLLARS'; // String instead of number

      const result = validateEvent('workflow.submitted.v1', rawEvent);
      expect(result.valid).toBe(false);
      expect(result.errors?.some((e) => e.includes('amount'))).toBe(true);
    });
  });

  describe('4. Schema Evolution Compatibility Gate (Confluent / Registry Rules)', () => {
    it('verifies that schema evolution follows BACKWARD compatibility rules', () => {
      const v1Compiled = ajv.compile(workflowApprovedSchema);

      // Simulating a V2 Producer emitting to a V1 Consumer
      const v2MessageWithExtraField = {
        specversion: '1.0',
        id: '9f8e7d6c-5b4a-3210-9876-fedcba098765',
        source: 'workflow-api',
        type: 'workflow.approved.v1',
        datacontenttype: 'application/json',
        time: '2026-09-22T10:00:00.000Z',
        data: {
          workflowId: '550e8400-e29b-41d4-a716-446655440000',
          tenantId: 'tenant-alpha',
          title: 'Q3 Enterprise Architecture Tooling',
          type: 'EXPENSE',
          amount: 45000,
          requesterId: 'user-alice',
          requesterName: 'Alice Johnson',
          actorId: 'user-bob',
          actorName: 'Bob Smith',
          previousStatus: 'PENDING',
          currentStatus: 'APPROVED',
          timestamp: '2026-09-22T10:00:00.000Z',
          // New optional V2 fields:
          approvedBudgetDepartment: 'ENGINEERING',
          approverIpAddress: '192.168.1.100',
        },
      };

      // V1 Consumer successfully validates V2 message
      const isCompatible = v1Compiled(v2MessageWithExtraField);
      expect(isCompatible).toBe(true);
    });
  });
});
