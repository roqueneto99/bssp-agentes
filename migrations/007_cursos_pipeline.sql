-- =================================================================
-- Migration 007 — Catálogo de cursos + IDs de board/list nos leads
--
-- Objetivos:
--   1. Tabela `cursos` com valor de matrícula e mensalidades — alimenta o
--      campo "Valor matrícula" do card e o KPI "Valor Potencial".
--   2. Persistir hablla_board_id e hablla_list_id em leads — permite JOIN
--      direto com `cursos` (board) e `hablla_id_map` (list/etapa).
--
-- Tudo idempotente.
-- =================================================================

CREATE TABLE IF NOT EXISTS cursos (
    id                   BIGSERIAL PRIMARY KEY,
    hablla_board_id      VARCHAR(64) NOT NULL UNIQUE,
    codigo               VARCHAR(32),                -- opcional, pra cross-ref interno
    nome                 VARCHAR(255) NOT NULL,
    valor_matricula_brl  NUMERIC(10, 2),
    mensalidades         INTEGER,
    ativo                BOOLEAN NOT NULL DEFAULT TRUE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_cursos_ativo ON cursos (ativo) WHERE ativo;

ALTER TABLE leads
    ADD COLUMN IF NOT EXISTS hablla_board_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS hablla_list_id  VARCHAR(64);

CREATE INDEX IF NOT EXISTS ix_leads_hablla_board_id
    ON leads (hablla_board_id)
    WHERE hablla_board_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_leads_hablla_list_id
    ON leads (hablla_list_id)
    WHERE hablla_list_id IS NOT NULL;
