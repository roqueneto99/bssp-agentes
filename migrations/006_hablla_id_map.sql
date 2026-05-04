-- =================================================================
-- Migration 006 — Tabela manual hablla_id_map
--
-- O token de integração Hablla não tem permissão pra listar boards/lists.
-- Como a BSSP tem poucos cursos (~5) e poucas etapas por funil, usamos
-- uma tabela manual pra mapear hablla_id -> nome humano.
--
-- Cadastro via:
--   POST /api/admin/sync/hablla/map
--     {hablla_id, type, name}
--   type ∈ ('board', 'list', 'user', 'sector')
--
-- O sync usa essa tabela como FALLBACK quando não consegue resolver via API.
-- =================================================================

CREATE TABLE IF NOT EXISTS hablla_id_map (
    id          BIGSERIAL PRIMARY KEY,
    hablla_id   VARCHAR(64)  NOT NULL,
    type        VARCHAR(16)  NOT NULL,  -- board | list | user | sector
    name        VARCHAR(255) NOT NULL,
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_hablla_id_type UNIQUE (hablla_id, type),
    CONSTRAINT ck_hablla_type CHECK (type IN ('board', 'list', 'user', 'sector'))
);

CREATE INDEX IF NOT EXISTS ix_hablla_id_map_type ON hablla_id_map (type);
