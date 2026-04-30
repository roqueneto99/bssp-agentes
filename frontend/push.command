#!/bin/bash
# Script de bootstrap do repo bssp-frontend.
# Execute com duplo clique no Finder, ou via Terminal:
#   bash push.command
#
# Antes de rodar:
#   1. Crie o repo PRIVADO em https://github.com/new com nome: bssp-frontend
#   2. NÃO inicialize com README, .gitignore ou license (este script faz tudo).

set -e
cd "$(dirname "$0")"

echo "==> 1. Limpando .git fantasma deixado pelo sandbox"
rm -rf .git

echo "==> 2. Removendo arquivos temporários do pnpm"
rm -f _tmp_* 2>/dev/null || true

echo "==> 3. Inicializando repositório"
git init -b main

git config user.email "roque@bssp.com.br"
git config user.name "Roque Neto"

echo "==> 4. Criando primeiro commit"
git add -A
git commit -m "sprint 0: bootstrap do frontend (Next.js + TS + Tailwind + shadcn/ui + auth Credentials)

- Next.js 14 (App Router) + TypeScript estrito
- Tailwind CSS + shadcn/ui (Button, Card, Avatar, Badge, DropdownMenu, Input, Label, Skeleton)
- NextAuth v5 com Credentials provider (email + senha + bcrypt)
- Split de auth: lib/auth.config.ts (edge) + lib/auth.ts (Node)
- USERS_JSON em env var como source de usuarios
- Script pnpm hash para gerar bcrypt
- RBAC com 4 papeis (admin/sales/marketing/executive)
- Sidebar filtrada por role + Topbar
- 12 paginas placeholder (uma por modulo do roadmap)
- Middleware protege rotas privadas
- Configs de deploy Railway + CI GitHub Actions + Vitest + README"

echo "==> 5. Adicionando remote e fazendo push"
git remote add origin git@github.com:roqueneto99/bssp-frontend.git || git remote set-url origin git@github.com:roqueneto99/bssp-frontend.git
git push -u origin main

echo ""
echo "Pronto. Repo publicado em https://github.com/roqueneto99/bssp-frontend"
