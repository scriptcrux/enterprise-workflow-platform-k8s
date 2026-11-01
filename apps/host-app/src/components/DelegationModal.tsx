import React, { useState, useEffect, useCallback } from 'react';
import { useAuth, PRESET_USERS } from '../context/AuthContext.js';
import { buildAuthHeaders, tracedFetch } from '../utils/api.js';
import { Delegation } from '@workflow/shared-types';
import { X, Calendar, UserCheck, Clock, ShieldCheck, PlusCircle, CheckCircle2, AlertCircle } from 'lucide-react';

interface DelegationModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export const DelegationModal: React.FC<DelegationModalProps> = ({ isOpen, onClose }) => {
  const { tenantId, currentUser, apiUrl } = useAuth();
  const [delegations, setDelegations] = useState<Delegation[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [submitting, setSubmitting] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  // Form State
  const defaultDelegatee = PRESET_USERS.find((u) => u.id !== currentUser.id)?.id || 'user-carol';
  const [delegateeId, setDelegateeId] = useState<string>(defaultDelegatee);

  const formatDatetimeLocal = (date: Date) => {
    const pad = (n: number) => (n < 10 ? `0${n}` : `${n}`);
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  };

  const now = new Date();
  const inOneWeek = new Date(Date.now() + 7 * 24 * 3600 * 1000);

  const [validFrom, setValidFrom] = useState<string>(formatDatetimeLocal(now));
  const [validUntil, setValidUntil] = useState<string>(formatDatetimeLocal(inOneWeek));
  const [reason, setReason] = useState<string>('Annual Leave / Out of Office');

  const fetchDelegations = useCallback(async () => {
    if (!isOpen) return;
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await tracedFetch(
        `${apiUrl}/api/v1/delegations?delegatorId=${currentUser.id}`,
        {
          headers: buildAuthHeaders(tenantId, currentUser),
        }
      );
      const json = await res.json();
      if (json.success && json.data) {
        setDelegations(json.data);
      }
    } catch (err: any) {
      console.error('Failed to fetch delegations:', err);
    } finally {
      setLoading(false);
    }
  }, [apiUrl, tenantId, currentUser, isOpen]);

  useEffect(() => {
    if (isOpen) {
      fetchDelegations();
    }
  }, [isOpen, fetchDelegations]);

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setErrorMsg(null);
    setSuccessMsg(null);

    try {
      const fromDate = new Date(validFrom);
      const untilDate = new Date(validUntil);

      if (untilDate <= fromDate) {
        throw new Error('End date must be after start date');
      }

      const res = await tracedFetch(`${apiUrl}/api/v1/delegations`, {
        method: 'POST',
        headers: buildAuthHeaders(tenantId, currentUser),
        body: JSON.stringify({
          delegatorId: currentUser.id,
          delegateeId,
          validFrom: fromDate.toISOString(),
          validUntil: untilDate.toISOString(),
          reason,
        }),
      });

      const json = await res.json();
      if (!res.ok || !json.success) {
        throw new Error(json.error?.message || 'Failed to create delegation rule');
      }

      setSuccessMsg('Delegation successfully registered! Your proxy colleague can now approve on your behalf.');
      await fetchDelegations();
      setTimeout(() => setSuccessMsg(null), 4000);
    } catch (err: any) {
      setErrorMsg(err.message || 'Error creating delegation');
    } finally {
      setSubmitting(false);
    }
  };

  const getDelegateeName = (id: string) => {
    const user = PRESET_USERS.find((u) => u.id === id);
    return user ? `${user.name} (${user.title})` : id;
  };

  const getStatusBadge = (d: Delegation) => {
    const from = new Date(d.validFrom).getTime();
    const until = new Date(d.validUntil).getTime();
    const cur = Date.now();

    if (cur < from) {
      return (
        <span style={{ backgroundColor: '#fef3c7', color: '#b45309', padding: '2px 8px', borderRadius: '4px', fontSize: '11px', fontWeight: 700 }}>
          SCHEDULED
        </span>
      );
    } else if (cur > until) {
      return (
        <span style={{ backgroundColor: '#f1f5f9', color: '#64748b', padding: '2px 8px', borderRadius: '4px', fontSize: '11px', fontWeight: 700 }}>
          EXPIRED
        </span>
      );
    }
    return (
      <span style={{ backgroundColor: '#dcfce7', color: '#15803d', padding: '2px 8px', borderRadius: '4px', fontSize: '11px', fontWeight: 700 }}>
        ACTIVE NOW
      </span>
    );
  };

  return (
    <div
      data-testid="delegation-modal-backdrop"
      onClick={onClose}
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(15, 23, 42, 0.75)',
        backdropFilter: 'blur(4px)',
        zIndex: 1000,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '20px',
      }}
    >
      <div
        data-testid="delegation-modal-content"
        onClick={(e) => e.stopPropagation()}
        style={{
          backgroundColor: '#ffffff',
          borderRadius: '16px',
          width: '100%',
          maxWidth: '680px',
          maxHeight: '90vh',
          overflowY: 'auto',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
          display: 'flex',
          flexDirection: 'column',
          border: '1px solid #e2e8f0',
        }}
      >
        {/* Modal Header */}
        <div
          style={{
            padding: '20px 24px',
            borderBottom: '1px solid #e2e8f0',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            backgroundColor: '#f8fafc',
            borderTopLeftRadius: '16px',
            borderTopRightRadius: '16px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div
              style={{
                width: '40px',
                height: '40px',
                borderRadius: '10px',
                backgroundColor: '#eff6ff',
                color: '#2563eb',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              <UserCheck size={22} />
            </div>
            <div>
              <h2 style={{ fontSize: '18px', fontWeight: 700, color: '#0f172a', margin: 0 }}>
                Out-of-Office Delegations & Proxy Approvals
              </h2>
              <p style={{ fontSize: '13px', color: '#64748b', margin: '2px 0 0 0' }}>
                Delegate your workflow approval authority to a proxy colleague for {tenantId}
              </p>
            </div>
          </div>

          <button
            data-testid="close-delegation-modal-btn"
            onClick={onClose}
            style={{
              border: 'none',
              background: 'transparent',
              color: '#94a3b8',
              cursor: 'pointer',
              padding: '6px',
              borderRadius: '6px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <X size={20} />
          </button>
        </div>

        {/* Modal Body */}
        <div style={{ padding: '24px', display: 'flex', flexDirection: 'column', gap: '24px' }}>
          {errorMsg && (
            <div
              style={{
                backgroundColor: '#fef2f2',
                border: '1px solid #fee2e2',
                color: '#b91c1c',
                padding: '12px 16px',
                borderRadius: '8px',
                fontSize: '13px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
              }}
            >
              <AlertCircle size={16} />
              <span>{errorMsg}</span>
            </div>
          )}

          {successMsg && (
            <div
              style={{
                backgroundColor: '#f0fdf4',
                border: '1px solid #dcfce7',
                color: '#15803d',
                padding: '12px 16px',
                borderRadius: '8px',
                fontSize: '13px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
              }}
            >
              <CheckCircle2 size={16} />
              <span>{successMsg}</span>
            </div>
          )}

          {/* New Delegation Form */}
          <form
            onSubmit={handleSubmit}
            style={{
              backgroundColor: '#f8fafc',
              border: '1px solid #e2e8f0',
              borderRadius: '12px',
              padding: '20px',
              display: 'flex',
              flexDirection: 'column',
              gap: '16px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
              <PlusCircle size={16} color="#2563eb" />
              <h3 style={{ fontSize: '15px', fontWeight: 700, color: '#0f172a', margin: 0 }}>
                Set New Out-of-Office Delegation
              </h3>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <div>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 600, color: '#334155', marginBottom: '6px' }}>
                  Delegator (You)
                </label>
                <input
                  type="text"
                  disabled
                  value={`${currentUser.name} (${currentUser.title})`}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    border: '1px solid #cbd5e1',
                    backgroundColor: '#e2e8f0',
                    color: '#64748b',
                    fontSize: '13px',
                  }}
                />
              </div>

              <div>
                <label style={{ display: 'block', fontSize: '13px', fontWeight: 600, color: '#334155', marginBottom: '6px' }}>
                  Proxy Colleague (Delegatee)
                </label>
                <select
                  data-testid="delegation-delegatee-select"
                  value={delegateeId}
                  onChange={(e) => setDelegateeId(e.target.value)}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    border: '1px solid #cbd5e1',
                    backgroundColor: '#ffffff',
                    fontSize: '13px',
                    color: '#0f172a',
                    outline: 'none',
                  }}
                >
                  {PRESET_USERS.filter((u) => u.id !== currentUser.id).map((u) => (
                    <option key={u.id} value={u.id}>
                      {u.name} — {u.title}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <div>
                <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', fontWeight: 600, color: '#334155', marginBottom: '6px' }}>
                  <Calendar size={14} color="#64748b" />
                  Valid From
                </label>
                <input
                  type="datetime-local"
                  data-testid="delegation-valid-from"
                  value={validFrom}
                  onChange={(e) => setValidFrom(e.target.value)}
                  required
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    border: '1px solid #cbd5e1',
                    backgroundColor: '#ffffff',
                    fontSize: '13px',
                    color: '#0f172a',
                    outline: 'none',
                  }}
                />
              </div>

              <div>
                <label style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', fontWeight: 600, color: '#334155', marginBottom: '6px' }}>
                  <Clock size={14} color="#64748b" />
                  Valid Until
                </label>
                <input
                  type="datetime-local"
                  data-testid="delegation-valid-until"
                  value={validUntil}
                  onChange={(e) => setValidUntil(e.target.value)}
                  required
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    borderRadius: '6px',
                    border: '1px solid #cbd5e1',
                    backgroundColor: '#ffffff',
                    fontSize: '13px',
                    color: '#0f172a',
                    outline: 'none',
                  }}
                />
              </div>
            </div>

            <div>
              <label style={{ display: 'block', fontSize: '13px', fontWeight: 600, color: '#334155', marginBottom: '6px' }}>
                Reason for Delegation
              </label>
              <input
                type="text"
                data-testid="delegation-reason-input"
                placeholder="e.g. Annual PTO, Conference attendance, Medical Leave"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                required
                style={{
                  width: '100%',
                  padding: '8px 12px',
                  borderRadius: '6px',
                  border: '1px solid #cbd5e1',
                  backgroundColor: '#ffffff',
                  fontSize: '13px',
                  color: '#0f172a',
                  outline: 'none',
                }}
              />
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '4px' }}>
              <button
                type="submit"
                disabled={submitting}
                data-testid="create-delegation-submit-btn"
                className="btn btn-primary"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  padding: '8px 18px',
                  fontSize: '13px',
                  fontWeight: 600,
                  backgroundColor: '#2563eb',
                  color: '#ffffff',
                  borderRadius: '6px',
                  border: 'none',
                  cursor: submitting ? 'not-allowed' : 'pointer',
                }}
              >
                <ShieldCheck size={16} />
                {submitting ? 'Registering...' : 'Save Delegation Authority'}
              </button>
            </div>
          </form>

          {/* Active Delegations List */}
          <div>
            <h3 style={{ fontSize: '15px', fontWeight: 700, color: '#0f172a', marginBottom: '12px' }}>
              Your Active & Scheduled Delegations
            </h3>

            {loading ? (
              <div style={{ padding: '20px', textAlign: 'center', color: '#64748b', fontSize: '13px' }}>
                Loading delegations...
              </div>
            ) : delegations.length === 0 ? (
              <div
                style={{
                  padding: '24px',
                  border: '1px dashed #cbd5e1',
                  borderRadius: '8px',
                  textAlign: 'center',
                  color: '#64748b',
                  fontSize: '13px',
                }}
              >
                No active delegations configured. Fill out the form above to delegate your approval authority when out of office.
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {delegations.map((d) => (
                  <div
                    key={d.id}
                    data-testid="delegation-record-card"
                    style={{
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px',
                      padding: '14px 16px',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      backgroundColor: '#ffffff',
                    }}
                  >
                    <div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                        <span style={{ fontWeight: 700, fontSize: '14px', color: '#0f172a' }}>
                          Proxy: {getDelegateeName(d.delegateeId)}
                        </span>
                        {getStatusBadge(d)}
                      </div>
                      <div style={{ fontSize: '12px', color: '#64748b', display: 'flex', gap: '16px' }}>
                        <span>
                          <strong>Window:</strong> {new Date(d.validFrom).toLocaleDateString()} — {new Date(d.validUntil).toLocaleDateString()}
                        </span>
                        {d.reason && (
                          <span>
                            <strong>Reason:</strong> {d.reason}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
