import NextAuth from 'next-auth';
import Credentials from 'next-auth/providers/credentials';
import bcrypt from 'bcryptjs';
import { z } from 'zod';
import { authConfig } from '@/lib/auth.config';
import { findUserByEmail } from '@/lib/users';

const CredsSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
});

export const { handlers, auth, signIn, signOut } = NextAuth({
  ...authConfig,
  providers: [
    Credentials({
      name: 'BSSP',
      credentials: {
        email: { label: 'E-mail', type: 'email' },
        password: { label: 'Senha', type: 'password' },
      },
      authorize: async (raw) => {
        const parsed = CredsSchema.safeParse(raw);
        if (!parsed.success) return null;
        const { email, password } = parsed.data;
        const user = findUserByEmail(email);
        if (!user) return null;
        const ok = await bcrypt.compare(password, user.passwordHash);
        if (!ok) return null;
        return {
          id: user.email,
          email: user.email,
          name: user.name ?? user.email,
        };
      },
    }),
    // ---- Google SSO (preparado, ativar em sprint futura) ----
    // import Google from 'next-auth/providers/google';
    // Google({
    //   clientId: process.env.GOOGLE_CLIENT_ID ?? '',
    //   clientSecret: process.env.GOOGLE_CLIENT_SECRET ?? '',
    // }),
  ],
  callbacks: {
    ...authConfig.callbacks,
    async jwt({ token, user, trigger }) {
      if (user?.email) {
        token.email = user.email.toLowerCase();
        token.name = user.name ?? user.email;
      }
      if (!token.role || trigger === 'update') {
        const lookup = findUserByEmail((token.email as string | undefined) ?? '');
        if (lookup) token.role = lookup.role;
      }
      return token;
    },
  },
});
