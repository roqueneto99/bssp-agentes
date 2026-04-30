# Notas sobre o Sync RDStation → Postgres

Documento de referência para o comportamento real das APIs do RDStation,
com base em verificações automatizadas via `sync_job.py`.

## Limitação da API de Segmentação

**Endpoint:** `GET /platform/segmentations/:id/contacts`
**Usado em:** `src/integrations/rdstation/client.py::get_segmentation_contacts`
**Consumido por:** `src/database/sync.py::full_sync`, `incremental_sync`

**Campos devolvidos por contato (verificado em 2026-04-20, 613 contatos inspecionados):**

| Campo | Preenchimento |
|---|---|
| `email` | 100% |
| `name` | 100% |
| `last_conversion_date` | 100% |
| `created_at` | 100% |
| `uuid` | 100% |
| `first_conversion_date` | 0% (sempre vazio) |
| `lifecycle_stage` | 0% (sempre vazio) |
| `tags` | 0% (sempre vazio) |
| `mobile_phone` / `personal_phone` | 0% (sempre vazio) |
| `city` / `state` / `country` | 0% (sempre vazio) |
| `fit_score` / `interest_score` | 0% (sempre vazio) |
| `job_title` / `company_name` | 0% (sempre vazio) |

**Consequência prática:** `_contact_to_lead_dict` em `sync.py` está preparado
para ler esses campos (usa `.get()` com default), mas eles não chegam pela
segmentação. O resultado é que a tabela `leads` fica com NULL nesses campos
se o único caminho de escrita for o `sync_job`.

## Caminhos de enriquecimento disponíveis

### (a) Endpoint individual (`get_contact`)

`GET /platform/contacts/email/:email` devolve o payload completo, incluindo
`tags`, `lifecycle_stage`, `fit_score`, `interest_score`, telefones, legal_bases
e campos customizados (`cf_*`).

Custo: 1 requisição por lead. Com rate limit Pro (120 burst + 2 req/s),
enriquecer 364K leads em carga completa levaria ~50 horas. Para o incremental
diário (~500 leads/24h), é trivial: ~4 minutos.

### (b) Webhook (`/api/webhooks/rdstation`)

Já implementado em `src/webhooks/`. Quando o RDStation dispara
`CONVERSION`, `OPPORTUNITY`, `MARKED_AS_OPPORTUNITY`, envia o contato
completo no payload. Se o webhook estiver ligado na conta RD, ele é a
fonte de verdade mais barata e em tempo real.

**Recomendação:** priorizar webhook. Se estiver indisponível ou degradado,
cair para enrich_job que percorre leads com `lifecycle_stage IS NULL`
ou `synced_at < NOW() - interval '7 days'` e chama `get_contact`.

## Volume e cadência observados (2026-04-20)

- Total na base: 364.463 (crescendo ~10 leads/h, ~240/dia).
- Janela incremental de 12h: 120 contatos únicos.
- Tempo de captura (1 página, 125 contatos): ~1s via `httpx`.
- Overlap recomendado entre runs: ≥ 20 min (para cobrir jitter de cron).

## Histórico de verificações

| Data | Run | Total RD | Capturado | Janela |
|---|---|---|---|---|
| 2026-04-20 00:21 BRT | tarefa agendada | 364.381 | 493 (24h) | 2026-04-19 00:22 → 2026-04-20 00:15 |
| 2026-04-20 08:53 BRT | tarefa agendada | 364.463 | 120 (12h) | 2026-04-19 23:55 → 2026-04-20 08:49 |

Sobreposição entre os dois runs: ~20 min. Nenhum buraco.
