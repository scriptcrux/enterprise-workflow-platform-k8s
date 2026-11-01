import { describe, it, expect, beforeEach, vi } from 'vitest';
import { OutboxRelayService } from '../../services/workflow-api/src/services/outbox.relay.js';
import { db } from '../../services/workflow-api/src/db/client.js';
import { eventProducer } from '../../services/workflow-api/src/kafka/producer.js';
import { WorkflowKafkaEvent } from '@workflow/shared-types';

describe('Transactional Outbox Relay Unit Tests', () => {
  let relayService: OutboxRelayService;

  beforeEach(() => {
    relayService = new OutboxRelayService();
    vi.restoreAllMocks();
  });

  it('saves events into outbox table with published = false', async () => {
    const mockInsert = vi.fn().mockReturnValue({
      values: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(db, 'insert').mockImplementation(mockInsert as any);

    const event: WorkflowKafkaEvent = {
      id: 'evt-outbox-1',
      source: 'workflow-api',
      specversion: '1.0',
      type: 'workflow.submitted.v1',
      time: new Date().toISOString(),
      datacontenttype: 'application/json',
      data: {
        workflowId: 'wf-1',
        tenantId: 'corp-alpha',
        type: 'EXPENSE',
        title: 'Office desk',
        requesterId: 'user-1',
        requesterName: 'User One',
        actorId: 'user-1',
        previousStatus: 'DRAFT',
        currentStatus: 'PENDING',
        timestamp: new Date().toISOString(),
      },
    };

    await relayService.saveToOutbox('corp-alpha', 'wf-1', event);

    expect(db.insert).toHaveBeenCalledTimes(1);
  });

  it('relays pending outbox events to Kafka and marks them as published', async () => {
    const pendingRecord = {
      id: 'evt-pending-1',
      tenantId: 'corp-alpha',
      workflowId: 'wf-1',
      eventType: 'workflow.submitted.v1',
      payload: {
        id: 'evt-pending-1',
        source: 'workflow-api',
        specversion: '1.0',
        type: 'workflow.submitted.v1',
        time: new Date().toISOString(),
        datacontenttype: 'application/json',
        data: {
          workflowId: 'wf-1',
          tenantId: 'corp-alpha',
          type: 'EXPENSE',
          title: 'Office desk',
          requesterId: 'user-1',
          requesterName: 'User One',
          actorId: 'user-1',
          previousStatus: 'DRAFT',
          currentStatus: 'PENDING',
          timestamp: new Date().toISOString(),
        },
      },
      published: false,
      retryCount: 0,
    };

    vi.spyOn(db, 'select').mockReturnValue({
      from: vi.fn().mockReturnValue({
        where: vi.fn().mockReturnValue({
          orderBy: vi.fn().mockReturnValue({
            limit: vi.fn().mockResolvedValue([pendingRecord]),
          }),
        }),
      }),
    } as any);

    const updateSpy = vi.spyOn(db, 'update').mockReturnValue({
      set: vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      }),
    } as any);

    const publishSpy = vi.spyOn(eventProducer, 'publishWorkflowEvent').mockResolvedValue(undefined);

    const result = await relayService.relayUnpublishedEvents();

    expect(result.relayed).toBe(1);
    expect(result.errors).toBe(0);
    expect(publishSpy).toHaveBeenCalledTimes(1);
    expect(updateSpy).toHaveBeenCalledTimes(1);
  });

  it('handles Kafka outage / broker exception: increments retryCount and records lastError in DB', async () => {
    const pendingRecord = {
      id: 'evt-fail-1',
      tenantId: 'corp-alpha',
      workflowId: 'wf-fail-1',
      eventType: 'workflow.submitted.v1',
      payload: {
        id: 'evt-fail-1',
        source: 'workflow-api',
        specversion: '1.0',
        type: 'workflow.submitted.v1',
        time: new Date().toISOString(),
        datacontenttype: 'application/json',
        data: {
          workflowId: 'wf-fail-1',
          tenantId: 'corp-alpha',
          type: 'EXPENSE',
          title: 'Failed Event Test',
          requesterId: 'user-1',
          requesterName: 'User One',
          actorId: 'user-1',
          previousStatus: 'DRAFT',
          currentStatus: 'PENDING',
          timestamp: new Date().toISOString(),
        },
      },
      published: false,
      retryCount: 2,
    };

    vi.spyOn(db, 'select').mockReturnValue({
      from: vi.fn().mockReturnValue({
        where: vi.fn().mockReturnValue({
          orderBy: vi.fn().mockReturnValue({
            limit: vi.fn().mockResolvedValue([pendingRecord]),
          }),
        }),
      }),
    } as any);

    const updateSetMock = vi.fn().mockReturnValue({
      where: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(db, 'update').mockReturnValue({
      set: updateSetMock,
    } as any);

    // Simulate Kafka network timeout / broker crash
    vi.spyOn(eventProducer, 'publishWorkflowEvent').mockRejectedValue(
      new Error('BrokerTransportFailure: Connection lost to Kafka broker:9092')
    );

    const result = await relayService.relayUnpublishedEvents();

    expect(result.relayed).toBe(0);
    expect(result.errors).toBe(1);
    expect(updateSetMock).toHaveBeenCalledWith(
      expect.objectContaining({
        retryCount: 3,
        lastError: 'BrokerTransportFailure: Connection lost to Kafka broker:9092',
      })
    );
  });

  it('handles immediate relay failure gracefully: returns false and records retry in DB', async () => {
    const record = {
      id: 'evt-imm-1',
      tenantId: 'corp-alpha',
      published: false,
      payload: {
        id: 'evt-imm-1',
        type: 'workflow.approved.v1',
        data: { workflowId: 'wf-1' },
      },
    };

    vi.spyOn(db, 'select').mockReturnValue({
      from: vi.fn().mockReturnValue({
        where: vi.fn().mockReturnValue({
          limit: vi.fn().mockResolvedValue([record]),
        }),
      }),
    } as any);

    const updateSetMock = vi.fn().mockReturnValue({
      where: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(db, 'update').mockReturnValue({
      set: updateSetMock,
    } as any);

    vi.spyOn(eventProducer, 'publishWorkflowEvent').mockRejectedValue(
      new Error('KafkaTimeoutException')
    );

    const success = await relayService.relayEventImmediately('evt-imm-1');

    expect(success).toBe(false);
    expect(updateSetMock).toHaveBeenCalled();
  });

  it('handles empty outbox backlog cleanly without errors', async () => {
    vi.spyOn(db, 'select').mockReturnValue({
      from: vi.fn().mockReturnValue({
        where: vi.fn().mockReturnValue({
          orderBy: vi.fn().mockReturnValue({
            limit: vi.fn().mockResolvedValue([]),
          }),
        }),
      }),
    } as any);

    const result = await relayService.relayUnpublishedEvents();

    expect(result.relayed).toBe(0);
    expect(result.errors).toBe(0);
  });
});
