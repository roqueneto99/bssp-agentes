import { z } from 'zod';
import { Role } from '@/lib/schemas';

const UserSchema = z.object({
  email: z.string().email(),
  /** bcrypt hash. Gerar com: pnpm hash "<senha>" */
  passwordHash: z.string().min(20),
  /** Nome de exibição. */
  name: z.string().optional(),
  role: Role,
});
export type User = z.infer<typeof UserSchema>;

const UsersListSchema = z.array(UserSchema);

let cached: User[] | null = null;

/**
 * Carrega a lista de usuários da env var USERS_JSON (string JSON).
 * Formato:
 *   USERS_JSON=[{"email":"roque@bssp.com.br","passwordHash":"$2a$10$...","role":"admin","name":"Roque"}]
 *
 * Retorna lista vazia se a env não estiver setada ou for inválida (com aviso no log).
 */
export function loadUsers(): User[] {
  if (cached) return cached;
  const raw = process.env.USERS_JSON;
  if (!raw) {
    console.warn('[auth] USERS_JSON não definida — nenhum usuário pode logar.');
    cached = [];
    return cached;
  }
  try {
    const parsed = JSON.parse(raw);
    cached = UsersListSchema.parse(parsed);
    return cached;
  } catch (e) {
    console.error('[auth] USERS_JSON inválido:', e);
    cached = [];
    return cached;
  }
}

export function findUserByEmail(email: string): User | undefined {
  const target = email.toLowerCase().trim();
  return loadUsers().find((u) => u.email.toLowerCase() === target);
}
