-- =================================================================
-- Migration 005 — Sync Hablla → leads
--
-- Adiciona colunas auxiliares preenchidas pelo módulo
-- src/sync/hablla_lead_sync.py:
--   - hablla_person_id      : id da pessoa correspondente no Hablla
--   - hablla_card_status    : "open" | "won" | "lost" | NULL
--   - hablla_em_atendimento : true se há card aberto OU service ativo
--   - hablla_synced_at      : timestamp do último sync (pra resync)
--
-- Os campos s3_estagio, s3_canal_preferido, s3_ultima_msg_em e
-- s3_ultima_resposta_em já existem desde a migration 002 — o sync
-- só passa a alimentá-los.
--
-- IMPORTANTE: Tudo idempotente (IF NOT EXISTS).
-- =================================================================

ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS hablla_person_id      VARCHAR(64),
    ADD COLUMN IF NOT EXISTS hablla_card_status    VARCHAR(16),
    ADD COLUMN IF NOT EXISTS hablla_em_atendimento BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS hablla_synced_at      TIMESTAMPTZ;

-- Índice pra acelerar o "pegar leads pendentes de sync"
CREATE INDEX IF NOT EXISTS ix_leads_hablla_synced_at
    ON leads (hablla_synced_at NULLS FIRST);

-- Índice pra acelerar render do kanban quando filtra por canal
CREATE INDEX IF NOT EXISTS ix_leads_s3_canal_preferido
    ON leads (s3_canal_preferido)
    WHERE s3_canal_preferido IS NOT NULL;
