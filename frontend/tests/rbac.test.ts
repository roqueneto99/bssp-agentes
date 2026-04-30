import { describe, it, expect } from 'vitest';
import { hasRole, hasAnyRole, homePathFor } from '@/lib/rbac';

describe('rbac', () => {
  it('admin satisfies any role', () => {
    expect(hasRole('admin', 'sales')).toBe(true);
    expect(hasRole('admin', 'marketing')).toBe(true);
    expect(hasRole('admin', 'executive')).toBe(true);
  });

  it('non-admin only matches own role', () => {
    expect(hasRole('sales', 'admin')).toBe(false);
    expect(hasRole('sales', 'marketing')).toBe(false);
    expect(hasRole('marketing', 'sales')).toBe(false);
  });

  it('hasAnyRole works for arrays', () => {
    expect(hasAnyRole('marketing', ['admin', 'marketing'])).toBe(true);
    expect(hasAnyRole('sales', ['admin', 'marketing'])).toBe(false);
  });

  it('homePathFor routes by role', () => {
    expect(homePathFor('admin')).toBe('/ops');
    expect(homePathFor('sales')).toBe('/leads');
    expect(homePathFor('marketing')).toBe('/cadencias');
    expect(homePathFor('executive')).toBe('/exec');
    expect(homePathFor(undefined)).toBe('/unauthorized');
  });
});
