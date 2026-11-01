import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

// Custom latency trends for granular endpoint benchmarking
const createDraftTrend = new Trend('workflow_create_draft_duration');
const submitWorkflowTrend = new Trend('workflow_submit_duration');
const approveWorkflowTrend = new Trend('workflow_approve_duration');
const getWorkflowTrend = new Trend('workflow_get_by_id_duration');
const listWorkflowsTrend = new Trend('workflow_list_duration');
const lifecycleSuccessRate = new Rate('workflow_lifecycle_success');

export const options = {
  stages: [
    { duration: '10s', target: 20 }, // ramp up to 20 users
    { duration: '40s', target: 50 }, // hold at 50 users
    { duration: '10s', target: 0 },  // ramp down to 0
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],                 // Error rate under 1%
    'workflow_create_draft_duration': ['p(95)<150', 'p(99)<300'], // Draft creation SLA
    'workflow_submit_duration': ['p(95)<150', 'p(99)<300'],       // Submission SLA
    'workflow_approve_duration': ['p(95)<150', 'p(99)<300'],      // Approval SLA
    'workflow_list_duration': ['p(95)<100', 'p(99)<250'],         // Listing SLA
    'workflow_lifecycle_success': ['rate>0.98'],                  // >98% complete workflow runs
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

export default function () {
  const tenantId = `tenant-${Math.floor(Math.random() * 5) + 1}`;
  const userId = `user-k6-${__VU}`;
  const approverId = `manager-k6-${__VU}`;

  const baseHeaders = {
    'Content-Type': 'application/json',
    'x-tenant-id': tenantId,
    'x-user-id': userId,
    'x-user-name': `Load Tester ${__VU}`,
  };

  let workflowId = null;
  let fullRunSuccessful = true;

  // 1. Create Workflow Draft
  const createPayload = JSON.stringify({
    type: 'EXPENSE',
    title: `Load Test Expense - VU ${__VU} - ${Date.now()}`,
    amount: Math.floor(Math.random() * 1000) + 50,
    currency: 'USD',
    description: 'Automated full-lifecycle performance test payload',
  });

  const createStart = Date.now();
  const createRes = http.post(`${BASE_URL}/api/v1/workflows`, createPayload, {
    headers: {
      ...baseHeaders,
      'idempotency-key': `k6-create-${__VU}-${__ITER}-${Date.now()}`,
    },
    tags: { endpoint: 'create_workflow' },
  });
  createDraftTrend.add(Date.now() - createStart);

  const createSuccess = check(createRes, {
    'create status is 201': (r) => r.status === 201,
    'response has workflow id': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.data && body.data.id !== undefined;
      } catch (e) {
        return false;
      }
    },
  });

  if (!createSuccess) {
    lifecycleSuccessRate.add(false);
    sleep(1);
    return;
  }

  workflowId = JSON.parse(createRes.body).data.id;

  // 2. Fetch Workflow by ID
  const getStart = Date.now();
  const getRes = http.get(`${BASE_URL}/api/v1/workflows/${workflowId}`, {
    headers: baseHeaders,
    tags: { endpoint: 'get_workflow_by_id' },
  });
  getWorkflowTrend.add(Date.now() - getStart);

  check(getRes, {
    'get by id status is 200': (r) => r.status === 200,
    'workflow status is DRAFT': (r) => {
      try {
        return JSON.parse(r.body).data.status === 'DRAFT';
      } catch (e) {
        return false;
      }
    },
  });

  // 3. Submit Workflow for Approval
  const submitStart = Date.now();
  const submitRes = http.post(
    `${BASE_URL}/api/v1/workflows/${workflowId}/submit`,
    JSON.stringify({ comment: 'Automated performance test submission' }),
    {
      headers: {
        ...baseHeaders,
        'idempotency-key': `k6-submit-${workflowId}`,
      },
      tags: { endpoint: 'submit_workflow' },
    }
  );
  submitWorkflowTrend.add(Date.now() - submitStart);

  const submitSuccess = check(submitRes, {
    'submit status is 200': (r) => r.status === 200,
    'status transitioned to PENDING': (r) => {
      try {
        return JSON.parse(r.body).data.status === 'PENDING';
      } catch (e) {
        return false;
      }
    },
  });

  if (!submitSuccess) {
    fullRunSuccessful = false;
  } else {
    // 4. Approve Workflow as Approver
    const approverHeaders = {
      ...baseHeaders,
      'x-user-id': approverId,
      'x-user-name': `Approver ${__VU}`,
      'idempotency-key': `k6-approve-${workflowId}`,
    };

    const approveStart = Date.now();
    const approveRes = http.post(
      `${BASE_URL}/api/v1/workflows/${workflowId}/approve`,
      JSON.stringify({ comment: 'Automated performance test approval' }),
      {
        headers: approverHeaders,
        tags: { endpoint: 'approve_workflow' },
      }
    );
    approveWorkflowTrend.add(Date.now() - approveStart);

    const approveSuccess = check(approveRes, {
      'approve status is 200': (r) => r.status === 200,
      'status transitioned to APPROVED': (r) => {
        try {
          return JSON.parse(r.body).data.status === 'APPROVED';
        } catch (e) {
          return false;
        }
      },
    });

    if (!approveSuccess) {
      fullRunSuccessful = false;
    }
  }

  // 5. Query Filtered Workflow List (Simulating Dashboard Polling)
  const listStart = Date.now();
  const listRes = http.get(`${BASE_URL}/api/v1/workflows?limit=10&status=APPROVED`, {
    headers: baseHeaders,
    tags: { endpoint: 'list_workflows' },
  });
  listWorkflowsTrend.add(Date.now() - listStart);

  check(listRes, {
    'list status is 200': (r) => r.status === 200,
    'list returns data array': (r) => {
      try {
        return Array.isArray(JSON.parse(r.body).data.items || JSON.parse(r.body).data);
      } catch (e) {
        return false;
      }
    },
  });

  lifecycleSuccessRate.add(fullRunSuccessful);
  sleep(1);
}
