# BSSP — Frontend de Gerenciamento das Squads

Interface multi-usuário (admin, sales, marketing, executive) para operar e
monitorar as squads de IA da BSSP. Substitui progressivamente o painel
antigo (`bssp-agentes/static/index.html`).

> **Localização no repo:** `bssp-agentes/frontend/` (monorepo).
> Backend Python continua na raiz do repo.

Stack: **Next.js 14** (App Router) + **TypeScript** estrito + **Tailwind** +
**shadcn/ui** + **NextAuth (Credentials)** + **TanStack Query** + **Zod**.

## 1. Estrutura

```
bssp-frontend/
├─ app/
│  ├─ (auth)/             login/, unauthorized/
│  ├─ (app)/              área autenticada (sidebar)
│  ├─ (admin)/            ops/ + settings/  (apenas admin)
│  ├─ api/auth/[...nextauth]/   handlers do NextAuth
│  ├─ api/health/         healthcheck do Railway
│  ├─ globals.css         tokens shadcn
│  └─ layout.tsx          Providers (Session + Query)
├─ components/ui/         shadcn (Button, Card, Avatar, Input, Label, ...)
├─ components/layout/     Sidebar, Topbar
├─ lib/                   auth.ts, users.ts, api.ts, rbac.ts, schemas.ts, utils.ts
├─ scripts/               hash.ts (gera bcrypt para USERS_JSON)
└─ tests/                 vitest
```

## 2. Setup local

Pré-requisitos: Node 20+, pnpm 9+.

```bash
cd bssp-agentes/frontend
pnpm install
cp .env.example .env.local
# Editar .env.local — ver seção 3.
pnpm dev
# Aplicação em http://localhost:3000
```

## 3. Variáveis de ambiente

`.env.local` mínimo para a fase atual (auth simples por e-mail + senha):

```
NEXTAUTH_URL=http://localhost:3000
NEXTAUTH_SECRET=<gerar com: openssl rand -base64 32>
BACKEND_URL=https://bssp-agentes-production.up.railway.app
USERS_JSON=[{"email":"roque@bssp.com.br","passwordHash":"<gerar com pnpm hash>","role":"admin","name":"Roque"}]
```

### 3.1 Adicionar usuários

Para gerar a hash bcrypt de uma senha:

```bash
pnpm hash "minha-senha"
# saída: $2a$10$abc...   (cole isso em passwordHash)
```

`USERS_JSON` é um array com objetos:

```json
[
  {"email": "roque@bssp.com.br",   "passwordHash": "$2a$10$...", "role": "admin",     "name": "Roque"},
  {"email": "diretor@bssp.com.br", "passwordHash": "$2a$10$...", "role": "executive", "name": "Diretor"},
  {"email": "vendedor@bssp.com.br","passwordHash": "$2a$10$...", "role": "sales"}
]
```

Quando quiser ativar SSO Google Workspace depois, basta adicionar
`GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` / `ALLOWED_EMAIL_DOMAIN` e
descomentar o bloco Google em `lib/auth.ts`. Toda a estrutura de
RBAC/JWT/sessão segue idêntica.

## 4. RBAC

Quatro papéis: `admin`, `sales`, `marketing`, `executive`. O role vem
do próprio `USERS_JSON`. Restrições de menu na Sidebar e nas rotas via
middleware + layout.

## 5. Deploy no Railway

1. Push do repo para GitHub.
2. Railway → New Service → from GitHub → escolher `bssp-frontend`.
3. Adicionar variáveis de ambiente (mesmas do `.env.example`).
4. `nixpacks.toml` cuida do build com pnpm + Node 20.
5. Healthcheck em `/api/health`.

Deploy automático a cada push em `main`.

## 6. Comandos úteis

```bash
pnpm dev          # desenvolvimento
pnpm build        # build de produção
pnpm start        # rodar build
pnpm lint         # ESLint
pnpm typecheck    # tsc --noEmit (estrito)
pnpm test         # vitest
pnpm hash "x"     # gera bcrypt de uma senha
```

## 7. Roadmap

Plano completo em `Frontend_BSSP_Plano_de_Projeto.docx` na raiz do
repositório principal. Resumo:

| Sprint | Janela | Escopo |
|--------|--------|--------|
| **S0** | 04 — 17 mai | **(esta) Bootstrap, auth Credentials, layout, placeholders.** |
| S1 | 18 — 31 mai | Lista de leads + Briefing. |
| S2 | 01 — 14 jun | Pipeline kanban + status comercial + auditoria v1. |
| S3 | 15 — 28 jun | Cadências + Templates (Marketing). |
| S4 | 29 jun — 12 jul | Análise + Dashboard executivo. |
| S5 | 13 — 26 jul | Operações (admin) + SSE feed. |
| S6 | 27 jul — 09 ago | Configurações + substituição do painel antigo + **migrar auth para Google SSO**. |
| S7 | 10 — 23 ago | Refinamentos pós-beta + hardening. |
