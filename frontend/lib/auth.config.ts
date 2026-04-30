import type { NextAuthConfig } from 'next-auth';
import type { JWT as DefaultJWT } from '@auth/core/jwt';
import type { Role } from '@/lib/rbac';

// Augmentation precisa de um import do módulo (acima) para o TS aceitar.
declare module 'next-auth' {
  interface Session {
    user: {
      role?: Role;
    } & import('next-auth').DefaultSession['user'];
  }
}

declare module '@auth/core/jwt' {
  interface JWT extends DefaultJWT {
    role?: Role;
    email?: string;
    name?: string;
  }
}

/**
 * Configuração compartilhada (sem providers que carregam código Node-only).
 * Usada pelo middleware (Edge Runtime) e como base do auth completo.
 */
export const authConfig: NextAuthConfig = {
  providers: [],
  pages: {
    signIn: '/login',
    error: '/login',
  },
  callbacks: {
    async session({ session, token }) {
      if (session.user && token.role) {
        session.user.role = token.role as Role;
      }
      if (session.user && token.email) {
        session.user.email = token.email as string;
      }
      if (session.user && token.name) {
        session.user.name = token.name as string;
      }
      return session;
    },
  },
  session: { strategy: 'jwt' },
  trustHost: true,
};
