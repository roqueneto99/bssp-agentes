# BSSP Agentes — Deploy no Railway (via GitHub)

Guia completo para colocar **painel + PostgreSQL + sync diário** no Railway.

> **Resultado final:** URL pública do painel, banco persistente com ~363K leads,
> sync incremental automático todo dia às **03:00 BRT**.

---

## Pré-requisitos

- [ ] Conta no [Railway](https://railway.app) (Trial ou Hobby $5/mês já cobre tudo)
- [ ] Conta no [GitHub](https://github.com)
- [ ] `git` instalado localmente (já tem no Mac)
- [ ] `.env` preenchido com credenciais RD Station (você já tem)

---

## Passo 1 — Criar repositório no GitHub

No seu Mac, dentro de `bssp-agentes/`:

```bash
cd "Automação - BSSP/bssp-agentes"
git init
git add .
git commit -m "Projeto inicial: painel BSSP + sync RD Station"
```

Depois, crie um repo novo (privado) em https://github.com/new com nome `bssp-agentes`.
Copie a URL que o GitHub mostra e:

```bash
git remote add origin git@github.com:SEU-USER/bssp-agentes.git
git branch -M main
git push -u origin main
```

> **Importante:** o `.gitignore` já exclui `.env`, `.venv/`, `__pycache__/` e
> a pasta `_arquivados_*/`. **Nunca** comite `.env`.

---

## Passo 2 — Criar projeto no Railway

1. Acesse https://railway.app/new
2. Clique **"Deploy from GitHub repo"** → autorize o Railway → escolha `bssp-agentes`
3. Railway detecta o `nixpacks.toml`/`railway.json` e inicia o primeiro build
4. O **primeiro deploy vai falhar** — é esperado, ainda falta o banco. Siga em frente.

---

## Passo 3 — Adicionar PostgreSQL

Dentro do projeto recém-criado no Railway:

1. Clique **"+ Create"** → **"Database"** → **"Add PostgreSQL"**
2. Aguarde o provisionamento (~30 segundos)
3. O Railway expõe automaticamente a variável `DATABASE_URL` no serviço do banco

---

## Passo 4 — Conectar o painel ao banco

No serviço do **painel** (não no Postgres):

1. Aba **"Variables"** → **"+ New Variable Reference"**
2. Referencie: `DATABASE_URL` → `${{Postgres.DATABASE_URL}}`
3. Adicione também (usar **"Raw Editor"** é mais rápido):

```env
RDSTATION_CLIENT_ID=xxxxx
RDSTATION_CLIENT_SECRET=xxxxx
RDSTATION_REFRESH_TOKEN=xxxxx
RDSTATION_PLAN=pro
LLM_API_KEY=xxxxx
HABLLA_API_TOKEN=xxxxx
HABLLA_WORKSPACE_ID=xxxxx
APP_ENV=production
LOG_LEVEL=INFO
```

4. O Railway re-deploya automaticamente. Dessa vez, o `release` phase roda
   `python -m scripts.run_migrations` e cria as 3 tabelas (`leads`, `execucoes`, `sync_log`).

---

## Passo 5 — Gerar URL pública

1. Serviço do painel → aba **"Settings"** → **"Networking"** → **"Generate Domain"**
2. Railway gera algo como `bssp-agentes-production.up.railway.app`
3. Teste: abrir a URL → a interface "BSSP — Squad Leads" deve carregar.

Healthcheck: `https://SUA-URL/api/db/mode` deve retornar `{"mode": "database"}`.

---

## Passo 6 — Carga inicial dos leads (~363K)

Uma única vez, para popular o banco. Duas opções:

### Opção A — Via shell temporário no Railway (recomendado)

1. Clique no serviço do painel → aba **"Deployments"** → "..." do deploy ativo → **"Shell"**
2. No terminal que abrir, rode:

```bash
python sync_job.py --full
```

3. Dura 15-30 min. Acompanhe o log. Ao terminar, verifique:

```bash
curl https://SUA-URL/api/db/stats
# → {"total_leads": 363240, ...}
```

### Opção B — Disparar via endpoint

```bash
curl -X POST "https://SUA-URL/api/db/sync?mode=full"
```

> Esse endpoint dispara em background — o job continua mesmo se você fechar o curl.

---

## Passo 7 — Sync diário automático (03:00 BRT)

Crie um **segundo serviço** no mesmo projeto Railway para o cron:

1. No projeto → **"+ Create"** → **"Empty Service"** (ou "Deploy from the same repo")
2. Nome: `bssp-sync-daily`
3. Aba **"Settings"**:
   - **Source:** mesmo repo GitHub (branch `main`)
   - **Start Command:** `python sync_job.py --hours 30`
   - **Cron Schedule:** `0 6 * * *`  ← 06:00 **UTC** = **03:00 BRT** ✅
4. Aba **"Variables"**: replique as mesmas variáveis do painel (use **"Shared Variables"**
   no projeto para evitar duplicação)
5. Save → o Railway só executa esse serviço no horário agendado, sem custo ocioso

> **Por que `--hours 30`?** Overlap de 6h cobre leads criados/editados em transições de dia
> e eventuais falhas de execução.

---

## Passo 8 — Monitoramento

### Logs
- Serviço do painel: **"Deployments"** → deploy ativo → **"View Logs"**
- Cron: **"Deployments"** do serviço `bssp-sync-daily` — cada execução gera um deploy

### Métricas no banco
```bash
# Total de leads
curl https://SUA-URL/api/db/stats

# Histórico de syncs (últimos 10)
curl https://SUA-URL/api/db/sync/history
```

### Consulta SQL direta
No serviço do Postgres → aba **"Data"** (UI do Railway) ou via `psql`:
```bash
railway run psql $DATABASE_URL
SELECT tipo, status, total_contacts, started_at, finished_at
  FROM sync_log ORDER BY started_at DESC LIMIT 10;
```

---

## Estimativa de custo

| Recurso | Plano Hobby ($5/mês) |
|---|---|
| Painel (web service) | ~$2-3/mês (sempre on) |
| PostgreSQL | ~$1-2/mês (armazena ~400K leads, <500MB) |
| Cron diário | ~$0,10/mês (roda 2-5 min/dia) |
| **Total** | **$3-5/mês** (dentro do Hobby) |

---

## Troubleshooting

**Build falha com "No module named asyncpg":**
- `requirements.txt` não subiu. Verifique `git status` e força um novo commit.

**Painel abre mas mostra "fonte: api" em vez de "database":**
- `DATABASE_URL` não foi referenciada. Volte ao Passo 4.

**Migration não rodou:**
- Veja logs do deploy ativo, procure por `[migrations]`. Se falhou, rode manual no shell:
  `python -m scripts.run_migrations`

**Cron não executa no horário:**
- Railway usa UTC. Confirme: `0 6 * * *` (não `0 3 * * *`).

---

## Rollback

Se quebrar algo:
1. Aba **"Deployments"** → clique num deploy anterior → **"Redeploy"**
2. Ou via git: `git revert HEAD && git push` — Railway re-deploya automático.
