import { describe, it, expect, beforeAll } from 'vitest';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { workflowSubmittedSchema, workflowApprovedSchema } from '@workflow/shared-schemas';

async function isSchemaRegistryAvailable(url: string): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 2000);
    const res = await fetch(`${url}/subjects`, { signal: controller.signal });
    clearTimeout(timeoutId);
    return res.ok;
  } catch {
    return false;
  }
}

const REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';

describe('Live Confluent Schema Registry Compatibility Suite (Port 8081)', async () => {
  const registryAvailable = await isSchemaRegistryAvailable(REGISTRY_URL);

  describe.skipIf(!registryAvailable)('Live Schema Registry Compatibility & Evolution Engine', () => {
    let registry: SchemaRegistry;
    const SUBJECT_SUBMITTED = 'workflow.events-submitted-test-value';
    const SUBJECT_APPROVED = 'workflow.events-approved-test-value';

    beforeAll(() => {
      registry = new SchemaRegistry({
        host: REGISTRY_URL,
        retry: { retries: 2 },
      });
    });

    it('successfully registers CloudEvents 1.0 JSON schemas in Confluent Schema Registry', async () => {
      const res = await registry.register(
        {
          type: SchemaType.JSON,
          schema: JSON.stringify(workflowSubmittedSchema),
        },
        { subject: SUBJECT_SUBMITTED }
      );

      expect(res.id).toBeGreaterThanOrEqual(1);
    });

    it('allows BACKWARD-compatible schema evolution (adding optional fields) in live registry', async () => {
      const v2ExtendedSchema = {
        ...workflowApprovedSchema,
        properties: {
          ...workflowApprovedSchema.properties,
          data: {
            ...workflowApprovedSchema.properties.data,
            properties: {
              ...(workflowApprovedSchema.properties.data as any).properties,
              costCenterCode: { type: 'string' },
              taxDeduction: { type: 'number' },
            },
          },
        },
      };

      const res = await registry.register(
        {
          type: SchemaType.JSON,
          schema: JSON.stringify(v2ExtendedSchema),
        },
        { subject: SUBJECT_APPROVED }
      );

      expect(res.id).toBeGreaterThanOrEqual(1);
    });

    it('encodes and decodes CloudEvent payload using Confluent Schema Registry wire format', async () => {
      const testEvent = {
        specversion: '1.0',
        id: '11111111-2222-3333-4444-555555555555',
        source: 'workflow-api',
        type: 'workflow.submitted.v1',
        datacontenttype: 'application/json',
        time: new Date().toISOString(),
        data: {
          workflowId: '22222222-3333-4444-5555-666666666666',
          tenantId: 'tenant-corp-a',
          type: 'EXPENSE',
          title: 'Wire Format Test Expense',
          amount: 12000,
          currency: 'USD',
          requesterId: 'user-alice',
          requesterName: 'Alice Johnson',
          actorId: 'user-alice',
          previousStatus: 'DRAFT',
          currentStatus: 'PENDING',
          timestamp: new Date().toISOString(),
        },
      };

      const registration = await registry.register(
        {
          type: SchemaType.JSON,
          schema: JSON.stringify(workflowSubmittedSchema),
        },
        { subject: `${SUBJECT_SUBMITTED}-roundtrip` }
      );

      const encodedBuffer = await registry.encode(registration.id, testEvent);
      expect(encodedBuffer).toBeInstanceOf(Buffer);
      // Confluent Wire Format: Byte 0 is magic byte (0x0), Bytes 1-4 are 32-bit big-endian schema ID
      expect(encodedBuffer[0]).toBe(0);

      const decodedPayload = await registry.decode(encodedBuffer);
      expect(decodedPayload.id).toBe(testEvent.id);
      expect(decodedPayload.data.workflowId).toBe(testEvent.data.workflowId);
      expect(decodedPayload.data.title).toBe(testEvent.data.title);
    });
  });

  if (!registryAvailable) {
    it.skip('Live Confluent Schema Registry suite (Registry not running on port 8081 in current environment)', () => {});
  }
});
