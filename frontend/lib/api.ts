import { auth } from '@/lib/auth';

const BACKEND = process.env.BACKEND_URL ?? '';
const TOKEN = process.env.BACKEND_INTERNAL_TOKEN ?? '';

interface FetchOptions extends RequestInit {
  /** Quando true, lança erro em status >= 400. Default: true. */
  throwOnError?: boolean;
}

/**
 * Wrapper para chamar o backend FastAPI. Adiciona Authorization header
 * com o token interno e o e-mail do usuário autenticado para o backend
 * resolver permissões e auditoria.
 */
export async function apiFetch<T = unknown>(
  path: string,
  options: FetchOptions = {},
): Promise<T> {
  if (!BACKEND) throw new Error('BACKEND_URL não configurado');
  const session = await auth();
  const headers = new Headers(options.headers);
  headers.set('Content-Type', 'application/json');
  if (TOKEN) headers.set('X-Internal-Token', TOKEN);
  if (session?.user?.email) headers.set('X-Acting-User', session.user.email);
  if (session?.user?.role)  headers.set('X-Acting-Role', session.user.role);

  const url = path.startsWith('http') ? path : `${BACKEND}${path}`;
  const res = await fetch(url, { ...options, headers, cache: 'no-store' });
  if (!res.ok && options.throwOnError !== false) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}
