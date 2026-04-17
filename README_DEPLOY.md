# BSSP Agentes — Deploy Railway do Zero

Roteiro enxuto e testado. Siga **na ordem** — cada passo só funciona com o anterior pronto.

---

## 0. Pré-requisitos

- [x] Repo GitHub criado: `github.com/roqueneto99/bssp-agentes` (privado)
- [x] Push do código feito (branch `main`)
- [ ] Conta no Railway (qualquer plano, Trial cobre setup)
- [ ] Credenciais RD Station + LLM + (opcional) Hablla em mãos

---

## 1. Limpar tentativas anteriores no Railway

Se já havia um projeto: dashboard Railway → abre o projeto → **Settings** (canto inferior esquerdo) → rola até **"Danger Zone"** → **Delete Project** → confirma o nome. Isso apaga tudo (painel + banco) sem custo e você começa fresh.

---

## 2. Criar projeto novo

1. https://railway.com/new
2. **"Deploy from GitHub repo"**
3. Autoriza o Railway a ler `roqueneto99/bssp-agentes` se for a primeira vez
4. Clica no repo → **"Deploy Now"**
5. O Railway cria um serviço com nome `web` (ou `bssp-agentes`) e começa a buildar.
6. **Não mexa ainda.** Deixa ele rodar. O primeiro deploy vai falhar (faltam DATABASE_URL e credenciais) — isso é normal.

---

## 3. Adicionar o Postgres ANTES de configurar variáveis

Na visão do projeto (canvas com os serviços):
1. Canto superior direito → **"+ Create"** → **"Database"** → **"Add PostgreSQL"**
2. Um segundo card aparece no canvas (cor azul). Espera ficar verde (~30s).

> **Por quê antes das variáveis:** você vai referenciar `${{Postgres.DATABASE_URL}}` no próximo passo, e essa referência só existe se o Postgres já estiver no projeto.

---

## 4. Configurar TODAS as variáveis no serviço web

**Fundamental:** essas variáveis vão no serviço **web** (card do GitHub), **não** no Postgres.

1. Clica no card **web** → aba **"Variables"** (no topo da página do serviço)
2. Clica em **"Raw Editor"** (botão no canto direito, acima da lista)
3. Apaga qualquer conteúdo existente e cola exatamente isso:

```env
DATABASE_URL=${{Postgres.DATABASE_URL}}
RDSTATION_CLIENT_ID=<cole do seu .env local>
RDSTATION_CLIENT_SECRET=<cole do seu .env local>
RDSTATION_REFRESH_TOKEN=<cole do seu .env local>
RDSTATION_PLAN=pro
RDSTATION_WEBHOOK_SECRET=<cole do seu .env local>
LLM_API_KEY=<cole do seu .env local>
APP_ENV=production
LOG_LEVEL=INFO
```

> **Importante:** os valores reais ficam apenas no seu `.env` local (que não é commitado) e no Railway. **Nunca** cole credenciais reais em arquivos versionados.

4. Clica **"Update Variables"** (canto inferior direito do editor).
5. **Valida que salvou:** fecha o Raw Editor → a tela deve mostrar 9 linhas de variáveis. Se mostrar menos, repete o passo 2-4.
6. Railway faz redeploy automático em ~10s.

---

## 5. Acompanhar o deploy

No serviço web → aba **"Deployments"** → clica no deploy "em execução" → aba **"Deploy Logs"**.

Você deve ver, em ordem:
1. `Running release: python -m scripts.run_migrations`
2. `Aplicando 001_create_tables.sql…` → `OK — 001_create_tables.sql`
3. `INFO: Started server process`
4. `INFO: Waiting for application startup.`
5. `RDStationClient inicializado (modo OAuth)`
6. `Modo DATABASE ativo — consultas via PostgreSQL`
7. `INFO: Application startup complete.`
8. `INFO: Uvicorn running on http://0.0.0.0:PORT`

Se parar em algum desses, copia o erro e me manda.

---

## 6. Gerar domain público

Serviço **web** → aba **"Settings"** → seção **"Networking"** → **"Generate Domain"**.

Anota a URL (algo como `bssp-agentes-production-XXXX.up.railway.app`).

**Validação:** abre no navegador:
- `https://SUA-URL/api/db/mode` → tem que devolver `{"mode": "database"}`
- `https://SUA-URL/api/db/stats` → `{"total_leads": 0, ...}` (zero é esperado aqui)
- `https://SUA-URL/` → carrega a UI "BSSP — Squad Leads" (vazia, sem leads)

---

## 7. Carga inicial — popular os ~363K leads

Esse passo dura 15-30 min e você só faz **uma vez**.

### Opção preferida — via Railway Shell
1. Serviço **web** → **Deployments** → deploy ativo → botão **"⋯"** → **"Shell"** (abre terminal no container)
2. Rode:
   ```bash
   python sync_job.py --full
   ```
3. Deixa rolar. Vai imprimir progresso tipo `Páginas 1-20/2906 | 2500 leads total | 125.0 leads/s`.
4. Quando terminar: `CARGA COMPLETA FINALIZADA`.
5. Valida no navegador: `https://SUA-URL/api/db/stats` → `{"total_leads": 363240, ...}`.

### Alternativa — via endpoint (se o shell travar)
```bash
curl -X POST "https://SUA-URL/api/db/sync?mode=full"
```
O job roda em background dentro do container do painel. Acompanhe pelos logs.

---

## 8. Configurar o cron diário (03:00 BRT)

1. Na visão do projeto → **"+ Create"** → **"GitHub Repo"** → escolhe `bssp-agentes` novamente (cria um segundo serviço do mesmo repo)
2. Nome do serviço: `bssp-sync-daily`
3. Aba **"Settings"** → **"Deploy"**:
   - **Start Command:** `python sync_job.py --hours 30`
   - **Cron Schedule:** `0 6 * * *`  ← 06:00 UTC = **03:00 BRT**
4. Aba **"Settings"** → **"Source"** → confirma branch `main`
5. Aba **"Variables"** → **"Raw Editor"** → cola o **mesmo bloco** de variáveis do Passo 4 (ou usa "Shared Variables" do projeto pra não duplicar).
6. Save. O serviço só roda no horário, sem custo ocioso.

**Valida amanhã de manhã:** Deployments do `bssp-sync-daily` deve ter um deploy novo entre 03:00 e 03:05 BRT, com log `Sync incremental finalizado: completed (N leads)`.

---

## Troubleshooting rápido

| Sintoma | Causa provável | Solução |
|---|---|---|
| Build trava em `pip install` com 145ms | `nixpacks.toml` customizado | Apague o arquivo e faça push |
| `ValueError: Informe api_key ou client_id + client_secret` | Variáveis não populoram no serviço web | Passo 4 — reabra o Raw Editor e confira |
| `/api/db/mode` retorna `"api"` em vez de `"database"` | `DATABASE_URL` ausente ou typo | Confere no Raw Editor se `${{Postgres.DATABASE_URL}}` está lá |
| Migration falhou com `relation already exists` | Sem problema — é idempotente | Ignorar |
| Cron não roda no horário | Railway usa UTC (não BRT) | Schedule deve ser `0 6 * * *`, não `0 3 * * *` |

---

## Custo estimado

| Recurso | Mês |
|---|---|
| web service (always on) | ~$2-3 |
| PostgreSQL (~500MB) | ~$1-2 |
| Cron diário | <$0,20 |
| **Total** | **~$3-5** |

Cabe no plano Hobby ($5 + uso).
