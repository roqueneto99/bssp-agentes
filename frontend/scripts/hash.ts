#!/usr/bin/env tsx
/**
 * Gera bcrypt hash de uma senha para colar em USERS_JSON.
 *
 * Uso:
 *   pnpm hash "senha-em-claro"
 */
import bcrypt from 'bcryptjs';

const password = process.argv[2];
if (!password) {
  console.error('Uso: pnpm hash "<senha>"');
  process.exit(1);
}

const hash = bcrypt.hashSync(password, 10);
console.log(hash);
