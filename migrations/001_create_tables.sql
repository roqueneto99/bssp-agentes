-- =========================================================
-- BSSP Agentes — Migration 001: Tabelas iniciais
-- PostgreSQL 15+
--
-- Uso:
--   psql $DATABASE_URL -f migrations/001_create_tables.sql
-- =========================================================

BEGIN;

-- -------------------------------------------------------
-- Tabela: leads
-- Espelho local dos contatos do RD Station (~363K linhas).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS leads (
    id              SERIAL PRIMARY KEY,
    uuid            VARCHAR(64)  UNIQUE,
    email           VARCHAR(320) NOT NULL UNIQUE,
    name            VARCHAR(512),

    -- Dados de contato
    personal_phone  VARCHAR(32),
    mobile_phone    VARCHAR(32),
    job_title       VARCHAR(256),
    company_name    VARCHAR(512),
    city            VARCHAR(128),
    state           VARCHAR(64),
    country         VARCHAR(64),
    linkedin        VARCHAR(512),
    website         VARCHAR(512),

    -- Datas do RD Station
    rd_created_at           TIMESTAMPTZ,
    last_conversion_date    TIMESTAMPTZ,
    first_conversion_date   TIMESTAMPTZ,

    -- Tags e bases legais (JSON arrays)
    tags            JSONB DEFAULT '[]'::jsonb,
    legal_bases     JSONB DEFAULT '[]'::jsonb,

    -- Custom fields do funil
    lifecycle_stage VARCHAR(64),
    fit_score       VARCHAR(16),
    interest_score  VARCHAR(16),

    -- Resultados Squad 1
    s1_temperatura        VARCHAR(32),
    s1_prioridade         VARCHAR(32),
    s1_area_principal     VARCHAR(128),
    s1_compliance         VARCHAR(32),
    s1_duplicados         INTEGER DEFAULT 0,
    s1_pode_seguir_squad2 BOOLEAN DEFAULT FALSE,
    s1_processado_em      TIMESTAMPTZ,

    -- Resultados Squad 2
    s2_score              FLOAT,
    s2_classificacao      VARCHAR(16),
    s2_rota               VARCHAR(64),
    s2_acoes              JSONB,
    s2_dimensoes          JSONB,
    s2_briefing           TEXT,
    s2_tags               JSONB DEFAULT '[]'::jsonb,
    s2_pode_seguir_squad3 BOOLEAN DEFAULT FALSE,
    s2_processado_em      TIMESTAMPTZ,

    -- JSON bruto original (backup)
    raw_data        JSONB,

    -- Controle de sync
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para queries do painel
CREATE INDEX IF NOT EXISTS ix_leads_uuid             ON leads (uuid);
CREATE INDEX IF NOT EXISTS ix_leads_rd_created_at    ON leads (rd_created_at);
CREATE INDEX IF NOT EXISTS ix_leads_last_conversion  ON leads (last_conversion_date);
CREATE INDEX IF NOT EXISTS ix_leads_s1_temperatura   ON leads (s1_temperatura);
CREATE INDEX IF NOT EXISTS ix_leads_s2_classificacao ON leads (s2_classificacao);
CREATE INDEX IF NOT EXISTS ix_leads_s2_score         ON leads (s2_score);
CREATE INDEX IF NOT EXISTS ix_leads_synced_at        ON leads (synced_at);

-- Índice GIN para busca em tags
CREATE INDEX IF NOT EXISTS ix_leads_tags_gin ON leads USING GIN (tags);

-- -------------------------------------------------------
-- Tabela: execucoes
-- Histórico de execuções dos agentes por lead.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS execucoes (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(320) NOT NULL,
    tipo            VARCHAR(32)  NOT NULL DEFAULT 'squad1',
    timestamp       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    total_ms        INTEGER,

    -- Resultado completo (JSON)
    agentes         JSONB,
    resumo_squad1   JSONB,
    resumo_squad2   JSONB,

    -- Campos denormalizados para queries rápidas
    temperatura     VARCHAR(32),
    score           FLOAT,
    classificacao   VARCHAR(16),
    success         BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS ix_exec_email_ts ON execucoes (email, timestamp);
CREATE INDEX IF NOT EXISTS ix_exec_tipo     ON execucoes (tipo);

-- -------------------------------------------------------
-- Tabela: sync_log
-- Controle de sincronizações com o RD Station.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_log (
    id              SERIAL PRIMARY KEY,
    tipo            VARCHAR(32) NOT NULL,  -- 'full', 'incremental'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    total_pages     INTEGER DEFAULT 0,
    total_contacts  INTEGER DEFAULT 0,
    new_contacts    INTEGER DEFAULT 0,
    updated_contacts INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    status          VARCHAR(16) DEFAULT 'running',  -- 'running', 'completed', 'failed'
    error_message   TEXT
);

-- -------------------------------------------------------
-- Função: auto-update updated_at
-- -------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_leads_updated_at
    BEFORE UPDATE ON leads
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

COMMIT;
