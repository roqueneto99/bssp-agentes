/**
 * RBAC: 4 papéis (admin, sales, marketing, executive) e helpers de
 * verificação. Os papéis são atribuídos pelo backend e propagados via
 * sessão NextAuth (token JWT).
 */
export const ROLES = ['admin', 'sales', 'marketing', 'executive'] as const;
export type Role = (typeof ROLES)[number];

const HIERARCHY: Record<Role, Role[]> = {
  admin: ['admin', 'sales', 'marketing', 'executive'],
  sales: ['sales'],
  marketing: ['marketing'],
  executive: ['executive'],
};

export function hasRole(role: Role | undefined, required: Role): boolean {
  if (!role) return false;
  return HIERARCHY[role]?.includes(required) ?? false;
}

export function hasAnyRole(role: Role | undefined, required: Role[]): boolean {
  return required.some((r) => hasRole(role, r));
}

/** Para qual rota redirecionar logo após o login. */
export function homePathFor(role: Role | undefined): string {
  switch (role) {
    case 'admin':     return '/ops';
    case 'sales':     return '/leads';
    case 'marketing': return '/cadencias';
    case 'executive': return '/exec';
    default:          return '/unauthorized';
  }
}
